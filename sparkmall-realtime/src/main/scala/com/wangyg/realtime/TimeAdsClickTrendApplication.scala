package com.wangyg.realtime

import java.text.SimpleDateFormat
import java.util.Date

import com.wangyg.sparkmall.comm.xbean._
import com.wangyg.sparkmall.common.xutil._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis
//
//object TimeAdvClickTrendApplication {
//
//  def main(args: Array[String]): Unit = {
//
//    val topic = "ads_log2";
//
//    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("DateAreaAdvCountTop3Application")
//
//    val streamingContext = new StreamingContext(sparkConf, Seconds(5))
//    streamingContext.sparkContext.setCheckpointDir("cp")
//    // 接收数据
//    val dStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext)
//
//    val messageDStream: DStream[KafkaMessage] = dStream.map(record => {
//      val datas = record.value().split(" ")
//      KafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
//    })
//
//    // 最近一小时广告点击量趋势
//    // 使用窗口函数来实现业务要求：窗口大小为1分钟，每10秒作为滑动步长
//    val windowDStream: DStream[KafkaMessage] = messageDStream.window(Minutes(1), Seconds(10))
//
//    // 通过窗口程序获取的数据结构：kafka
//    // 将kafka的数据转换为统计的数据结构（adv_time, 1L），（adv_time, 1L），（adv_time, 1L）
//    val advTimeToCountDStream: DStream[(String, Long)] = windowDStream.map(message => {
//
//      // 1，000 / 10=》0000 1L
//      // 3，000 / 10=》0 1L   ==> 00~10  3L
//      // 6，000 / 10=》0 1L
//      // 15，000 / 10=》10000 1L
//      // 18，000 / 10=》1 1L  ==> 10~20 2L
//      // 22, 0000 / 10 ==> 20000 1L ==> 20~30 1L
//
//
//      // 1 ==> 0~10
//      // 3 ==> 0~10
//
//
//      // 25 ==> 20~25
//
//      // 25秒 / 10 = 20000
//
//      // xxxxxxxxx01000 / 10000 + "0000"
//
//      val time = message.timestamp.toLong
//      val timeString = time / 10000 + "0000"
//
//      (message.adid + "_" + timeString, 1L)
//    })
//    // 将转换结构后的数据进行聚合
//    // (adv_time, sum)
//    val advTimeToSumReduceDStream: DStream[(String, Long)] = advTimeToCountDStream.reduceByKey(_+_)
//    // 将聚合后的数据进行转换：(adv,(time1, sum)), (adv, (time2, sum))
//    val advToTimeSumDStream: DStream[(String, (String, Long))] = advTimeToSumReduceDStream.map {
//      case (advTime, sum) => {
//        val ks = advTime.split("_")
//        (ks(0), (ks(1), sum))
//      }
//    }
//    // 将聚合后的数据进行排序最多6条
//    val groupDStream: DStream[(String, Iterable[(String, Long)])] = advToTimeSumDStream.groupByKey()
//
//    val resultDStream: DStream[(String, List[(String, Long)])] = groupDStream.mapValues(datas => {
//      datas.toList.sortWith {
//        case (left, right) => {
//          left._1 < right._1
//        }
//      }.take(6).map{
//        // xxxxxx : 100 ==> 01:15 => 100
//        case (time, sum) => {
//          val timeString = new SimpleDateFormat("mm:ss").format(new Date(time.toLong))
//          (timeString, sum)
//        }
//      }
//    })
//
//    // 将最终的数据保存到Redis中
//    import org.json4s.JsonDSL._
//
//    // 4.6 将最终的数据保存到Redis中
//    resultDStream.foreachRDD(rdd=>{
//      rdd.foreachPartition(datas=>{
//        val jedisClient: Jedis = RedisUtil.getJedisClient
//        for ((key, list) <- datas) {
//          val value = JsonMethods.compact(JsonMethods.render(list))
//          jedisClient.hset("time_adv_click_trend", key, value)
//        }
//
//        jedisClient.close()
//      })
//    })
//
//
//    streamingContext.start()
//    streamingContext.awaitTermination()
//  }
//}
object TimeAdvClickTrendApplication {
  def main(args: Array[String]): Unit = {
   //创建sparkconf
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TimeAdvClickTrendApplication")

    val streamingContext = new StreamingContext(conf, Seconds(5))

    //接收数据
    var topic = "ads_log2"
    val dStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext)

    //接收数据，将数据转化成样例类，这样方便进行操作
    val kafkaStream: DStream[KafkaMessage] = dStream.map(message => {
      val splits: Array[String] = message.value().split(" ")
      KafkaMessage(splits(0), splits(1), splits(2), splits(3), splits(4))
    })

    //最近一小时广告点击量趋势
    val windowDStream: DStream[KafkaMessage] = kafkaStream.window(Minutes(1), Seconds(5))

    //进行格式转化
    val advTimeCountDStream: DStream[(String, Long)] = windowDStream.map(message => {
      val time = message.timestamp.toLong //转为long类型
      val timeString: String = time / 10000 + "0000"
      (message.adid + "_" + timeString, 1L)
    })
    val reduceStream: DStream[(String, Long)] = advTimeCountDStream.reduceByKey(_+_)

    //广告和时间是1:n
    //将聚合后的数据进行转换(adv, (time1, sum) (adv, (time2, sum))
    reduceStream.map{
      case (advTime, sum)=>{

      }
    }



  }
}