package com.wangyg.realtime

import java.util

import com.wangyg.sparkmall.comm.xbean._
import com.wangyg.sparkmall.common.xutil._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

//object BlackListApplication {
//  def main(args: Array[String]): Unit = {
//    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BlackListApplication")
//
//    val ssc = new StreamingContext(conf, Seconds(5))
//
//    val topic = "ads_log"
//    //topic ssc
//    val dStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc)
//
//    //    println(dStream)
//
//    val messageDStream: DStream[KafkaMessage] = dStream.map(t => {
//      println("t.value=" + t.value())
//
//      val splits: Array[String] = t.value().split(" ")
//      //封装成kafkaMessage对象
//      KafkaMessage(splits(0), splits(1), splits(2), splits(3), splits(4))
//    })
//
//    //将封装后的对象进行处理，如果到达100次，则加入到黑名单中
//    // 将发送的数据进行验证，判断是否在黑名单列表中
//    //   从redis中获取黑名单列表:redis:Set. Smembers, sismember
//
//    //TODO 1==> driver执行1次
//    val filterDStream: DStream[KafkaMessage] = messageDStream.transform(rdd => {
//
//      val jedisClient: Jedis = RedisUtil.getJedisClient
//      val useridSet: util.Set[String] = jedisClient.smembers("blackList")
//      jedisClient.close()
//      val useridSetBroadcast: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(useridSet)
//      rdd.filter(message => {
//        // TODO 3 ==> Executor(N)
//        !useridSetBroadcast.value.contains(message.userid)
//      })
//    })
//
////    val filterDStream: DStream[KafkaMessage] = messageDStream.transform(rdd => {
////      val jedisClient: Jedis = RedisUtil.getJedisClient
////      val useridSet: util.Set[String] = jedisClient.smembers("blackList")
////      jedisClient.close()
////      val useridSetBroadcast: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(useridSet)
////      rdd.filter(message => {
////        !useridSetBroadcast.value.contains(message.userid)
////      })
////    })
//
//
//    filterDStream.foreachRDD(rdd=>{
//
//      rdd.foreachPartition(messages=>{
//        val innerJedisClient: Jedis = RedisUtil.getJedisClient
//        for ( message <- messages ) {
//          val field = message.userid + ":" + message.adid
//          innerJedisClient.hincrBy("user:advert:clickcount", field, 1)
//
//          // 4.5 累加次数后，没有到达阈值（100），继续访问
//          // 4.4.1 获取统计次数：redis:Hash:hget()
//          val clickCount = innerJedisClient.hget("user:advert:clickcount", field).toInt
//
//          // 4.6 累加次数后，到达阈值（100），将用户拉进黑名单，禁止访问系统
//          // 4.6.1将达到阈值的用户拉入黑名单：redis:Set.sadd
//          if ( clickCount >= 10 ) {
//            innerJedisClient.sadd("blackList", message.userid)
//          }
//        }
//        innerJedisClient.close()
//      })
//
//    })
//
//
//    //start()和await()
//    ssc.start()
//    ssc.awaitTermination()
//
//  }
//}


/*
  注意点： redis的配置中要修改两个地方，一： 60行 注掉#bind 127.0.0.1 81行 yes 该为no ,将保护模式关闭
 */
//
//object BlackListApplication {
//
//  def main(args: Array[String]): Unit = {
//
//    val topic = "ads_log";
//
//    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("BlackListApplication")
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
//
//    // 4.1 将发送的数据进行验证，判断是否在黑名单列表中
//    // 4.1.1 从redis中获取黑名单列表:redis:Set. Smembers, sismember
//
//
//    // 4.2 如果在黑名单中，那么用户无法执行后续操作
//    // 4.3 如果不在黑名单中，那可以继续执行业务
//    // TODO 1 ==> driver (1)
//    val filterDStream: DStream[KafkaMessage] = messageDStream.transform(rdd => {
//      // TODO 2 ==> drvier(N)
//      val jedisClient: Jedis = RedisUtil.getJedisClient
//      val useridSet: util.Set[String] = jedisClient.smembers("blackList")
//      jedisClient.close()
//      val useridSetBroadcast: Broadcast[util.Set[String]] = streamingContext.sparkContext.broadcast(useridSet)
//      rdd.filter(message => {
//        // TODO 3 ==> Executor(N)
//        !useridSetBroadcast.value.contains(message.userid)
//      })
//    })
//
//    // 4.4 将当前用户点击的广告次数进行统计
//    //     4.4.1 将用户点击广告次数在redis中进行统计：redis.Hash: hincrby
//    // key = user:advert:clickcount, field = user+adv value = ?
//    filterDStream.foreachRDD(rdd=>{
//
//      rdd.foreachPartition(messages=>{
//        val innerJedisClient: Jedis = RedisUtil.getJedisClient
//        for ( message <- messages ) {
//          val field = message.userid + ":" + message.adid
//          innerJedisClient.hincrBy("user:advert:clickcount", field, 1)
//
//          // 4.5 累加次数后，没有到达阈值（100），继续访问
//          // 4.4.1 获取统计次数：redis:Hash:hget()
//          val clickCount = innerJedisClient.hget("user:advert:clickcount", field).toInt
//
//          // 4.6 累加次数后，到达阈值（100），将用户拉进黑名单，禁止访问系统
//          // 4.6.1将达到阈值的用户拉入黑名单：redis:Set.sadd
//          if ( clickCount >= 10 ) {
//            innerJedisClient.sadd("blackList", message.userid)
//          }
//        }
//        innerJedisClient.close()
//      })
//
//    })
//
//    streamingContext.start()
//    streamingContext.awaitTermination()
//  }
//}