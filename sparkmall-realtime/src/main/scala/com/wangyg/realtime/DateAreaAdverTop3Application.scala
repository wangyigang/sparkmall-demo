package com.wangyg.realtime

import java.text.SimpleDateFormat
import java.util.Date

import com.wangyg.sparkmall.comm.xbean._
import com.wangyg.sparkmall.common.xutil._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

//实时数据分析：  每天各地区 top3 热门广告
/*
    分析过程 : 原始数据  date:area:city:ads
          希望得到数据 date:area: ads top3--去掉city 没有城市的概念
 */
object DateAreaAdverTop3Application {
  def main(args: Array[String]): Unit = {
    val topic = "ads_log2";

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("DateAreaAdverTop3Application")

    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 接收数据
    val dStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext)

    val messageDStream: DStream[KafkaMessage] = dStream.map(record => {
      val datas = record.value().split(" ")
      KafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
    })

    // 4.1 将kafka获取的数据进行拆分（date:area:city:ads, 1L）
    val dateAreaCityAdsDStream: DStream[(String, Long)] = messageDStream.map(message => {

      // 2019010110:10:10000 1L
      // 2019010110:10  1L
      // 2019010110 1L
      // 20190101 1L
      val date = new SimpleDateFormat("yyyy-MM-dd").format(new Date(message.timestamp.toLong))
      val key = date + ":" + message.area + ":" + message.city + ":" + message.adid
      (key, 1L)
    })

    streamingContext.sparkContext.setCheckpointDir("cp")
    val totalClickDStream: DStream[(String, Long)] = dateAreaCityAdsDStream.updateStateByKey {
      case (seq, cache) => {
        val sum = cache.getOrElse(0L) + seq.sum
        Option(sum)
      }
    }

    totalClickDStream.foreachRDD(rdd => {
      rdd.foreachPartition(datas => {
        val jedisClient: Jedis = RedisUtil.getJedisClient

        for ((key, sum) <- datas) {
          jedisClient.hset("date:area:city:ads", key, sum.toString)
        }
        jedisClient.close()
      })
    })

    //需求六： 每天各地区 top3 热门广告
    val dateAreaASsSumDstream: DStream[(String, Long)] = totalClickDStream.map {
      case (key, sum) => {
        //进行转换
        val splits: Array[String] = key.split(":")
        //date:area:adverse
        val newKey: String = splits(0) + ":" + splits(1) + ":" + splits(3)
        (newKey, sum) //返回新的元素，去掉城市属性
      }
    }
//    dateAreaASsSumDstream.print()
    //将转换后的数据进行聚合统计--去掉城市后，可能有多个相同的key存在，因为city属性去掉了，现在进行聚合
    val reduceDStream: DStream[(String, Long)] = dateAreaASsSumDstream.reduceByKey(_ + _)
    //按照时间:地区:广告--进行排序，排序字段或要求是什么，按照什么进行排序，按照题目需求来讲，是按照(每天， 每地区)这两个字段进行排序，可是，现在的key是(时间，地区，广告)
    //所以进行一次转变
    val dateAreaToadsumStream: DStream[(String, (String, Long))] = reduceDStream.map {
      case (key, sum) => {
        val splits: Array[String] = key.split(":")
        val newkey: String = splits(0) + ":" + splits(1) //newkey是时间，地区
        (newkey, (splits(2), sum))
      }
    }
    //结构发生改变后, 结构改变后，会有多个进行，所以进行分组排序
    val groupedStream: DStream[(String, Iterable[(String, Long)])] = dateAreaToadsumStream.groupByKey()
    //按照不同的时间和地区维度进行分组后的数据
    //然后进行统计top3
    //val resultDStream: DStream[(String, List[(String, Long)])] =
    val resultDStream: DStream[(String, List[(String, Long)])] = groupedStream.mapValues {
      case datas => {
        //按照点击量进行排序
        datas.toList.sortWith {
          case (left, right) => {
            left._2 > right._2
          }
        }
      }.take(3) //取出前三个
    }
    val resultMapDSstream: DStream[(String, Map[String, Long])] = resultDStream.map {
      case (key, list) => {
        (key, list.toMap)
      }
    }


    //向DStream保存数据--redis保存只能有三层，所以将结果保存为json对象
    import org.json4s.JsonDSL._

    resultMapDSstream.foreachRDD(rdd => {
      rdd.foreachPartition(datas => {
        val jedisClient: Jedis = RedisUtil.getJedisClient
        for ((key, sum) <- datas) {
          val ks: Array[String] = key.split(":")
          val str: String = JsonMethods.compact(JsonMethods.render(sum))
          jedisClient.hset("top3_ads_per_day:"+ks(0), ks(1), str)
        }
        jedisClient.close()
      })
    })

    //开启
    streamingContext.start()
    //等待数据
    streamingContext.awaitTermination()
  }
}
