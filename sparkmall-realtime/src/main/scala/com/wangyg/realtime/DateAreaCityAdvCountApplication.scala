package com.wangyg.realtime

import java.text.SimpleDateFormat
import java.util.Date

import com.wangyg.sparkmall.comm.xbean.KafkaMessage
import com.wangyg.sparkmall.common.xutil._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

///*
//    每天 每地区 每城市，  没广告的  点击数量
// */
object DateAreaCityAdvCountApplication {
  def main(args: Array[String]): Unit = {
    //创建sparkconf
    val conf = new SparkConf().setMaster("local[*]").setAppName("DateAreaCityAdvCountApplication")

    //创建Sparkstreamingcontext
    val streamContext = new StreamingContext(conf, Seconds(5))

    //通过kafka接收数据
    var topic = "ads_log1"
    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamContext)
    println(kafkaDstream)

    //进行转化数据为kafkamessage
    val kafkaMessage: DStream[KafkaMessage] = kafkaDstream.map(t => {
      val splits: Array[String] = t.value().split(" ")
      KafkaMessage(splits(0), splits(1), splits(2), splits(3), splits(4))
    })


    //转换成rdd后
    // timestamp:String, area:String, city:String, userid:String, adid:String
    //每天  timestamp  需要进行格式转化，转成每天即可 每地区area 每城市 city 每广告acid   点击数量 自己统计

    //先进行map操作转成自己想要的数据
    val mapStream: DStream[(String, Long)] = kafkaMessage.map(t => {
      val timestamp: String = t.timestamp
      val date: String = new SimpleDateFormat("yyyy-MM-dd").format(timestamp.toString)
      println(date)
      var key = date + ":" + t.area + ":" + t.city + ":" + t.adid
      (key, 1L)
    })

    mapStream.print()

    //然后进行统计聚合
    //两种方式--直接存储在redis中， 或者使用updateStatebykey 设置checkpointdir

    //方式一： 直接使用redis
    mapStream.foreachRDD(rdd => {
      rdd.foreachPartition(datas => {
        val jedisClient: Jedis = RedisUtil.getJedisClient
        println("jedisClient"+jedisClient)
        for (data <- datas) {

          jedisClient.hincrBy("date:area:city:ads", data._1, data._2)

        }
        jedisClient.close() //关闭资源
      })
    })

    //启动
    streamContext.start()
    //等待数据
    streamContext.awaitTermination()

  }
}

//
//object DateAreaCityAdvCountApplication {
//
//  def main(args: Array[String]): Unit = {
//
//    val topic = "ads_log2";
//
//    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("DateAreaCityAdvCountApplication")
//
//    val streamingContext = new StreamingContext(sparkConf, Seconds(5))
//
//    // 接收数据
//    val dStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext)
//
//    val messageDStream: DStream[KafkaMessage] = dStream.map(record => {
//      val datas = record.value().split(" ")
//      KafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
//    })
//    println(messageDStream)
//
//    // 4.1 将kafka获取的数据进行拆分（date:area:city:ads, 1L）
//    val dateAreaCityAdsDStream: DStream[(String, Long)] = messageDStream.map(message => {
//
//      // 2019010110:10:10000 1L
//      // 2019010110:10  1L
//      // 2019010110 1L
//      // 20190101 1L
//      val date = new SimpleDateFormat("yyyy-MM-dd").format(new Date(message.timestamp.toLong))
//      val key = date + ":" + message.area + ":" + message.city + ":" + message.adid
//      (key, 1L)
//    })
//
//    // 第一种实现方式 , 推荐使用
//    /*
//    val dateAreaCityAdsReduceDStream: DStream[(String, Long)] = dateAreaCityAdsDStream.reduceByKey(_+_)
//    dateAreaCityAdsReduceDStream.foreachRDD(rdd=>{
//        rdd.foreachPartition(datas=>{
//            val jedisClient: Jedis = RedisUtil.getJedisClient
//
//            for ((key, sum) <- datas) {
//                jedisClient.hincrBy("date:area:city:ads", key, sum)
//            }
//
//            jedisClient.close()
//        })
//    })
//    */
//
//    // 第二种实现方式
//    streamingContext.sparkContext.setCheckpointDir("cp")
//    // 4.2 将拆分数据进行聚合统计（date:area:city:ads， sum）
//    val totalClickDStream: DStream[(String, Long)] = dateAreaCityAdsDStream.updateStateByKey {
//      case (seq, cache) => {
//        val sum = cache.getOrElse(0L) + seq.sum
//        Option(sum)
//      }
//    }
//
//    // 4.3 将聚合的结果保存到检查点（有状态）中, 保存到Redis中
//    totalClickDStream.foreachRDD(rdd => {
//      rdd.foreachPartition(datas => {
//        val jedisClient: Jedis = RedisUtil.getJedisClient
//
//        for ((key, sum) <- datas) {
//          jedisClient.hset("date:area:city:ads", key, sum.toString)
//        }
//
//        jedisClient.close()
//      })
//    })
//
//    streamingContext.start()
//    streamingContext.awaitTermination()
//  }
//}
