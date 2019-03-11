package com.wangyg.realtime2

import java.util

import com.wangyg.sparkmall.comm.xbean._
import com.wangyg.sparkmall.common.xutil._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/*
    注意点：
    kafka是依赖于zookeeper的，如果zookeeper没有启动，那么kafka生产者和消费者就无法使用，
 */
object BlackListApplication2 {
  def main(args: Array[String]): Unit = {

    //环境准备
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BlackListApplication2")
    val ssc = new StreamingContext(conf, Seconds(5))

    val topic = "ads_log"
    //进行接收数据
    val dstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc)

    //接收到数据后，将数据进行封装成对象
    val kafkaMessageRDD: DStream[KafkaMessage] = dstream.map(t => {
      val splits: Array[String] = t.value().split(" ") //数据拆分以空格进行切分  ，然后进行封装成kafkamessage对象
      KafkaMessage(splits(0), splits(1), splits(2), splits(3), splits(4))
    })
    //封装转换成kafkaMessageRdd之后，和redis进行交互，获取黑名单数据，
    //统计没有在黑名单中的数据
    //使用transform方式进行过滤查找

    //使用transform的原因： transform在运行时运行在driver端，但是执行次数执行N次，每次Executor执行时都会执行transform中的
    //函数,从业务上来讲，黑名单的次数达到会直接加入到黑名单，但是Driver中只会执行一次，所以必须要使用transform
    val filterRDD: DStream[KafkaMessage] = kafkaMessageRDD.transform(rdd => {
      //创造共享变量
      val client: Jedis = RedisUtil.getJedisClient
      val set: util.Set[String] = client.smembers("blacklist")
      client.close() //关闭
      //封装为共享变量
      val broadcastBlackList: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(set)
      rdd.filter(b => {
        !broadcastBlackList.value.contains(b.userid)
      })

    })
    //每次在driver中过滤数据，然后进行业务判断,
    filterRDD.foreachRDD(t => {
      t.foreachPartition(beans => {
        val innerJedis: Jedis = RedisUtil.getJedisClient
        for (elem <- beans) {
          var key = "user:advert:count"
          innerJedis.hincrBy(key, elem.userid + ":" + elem.adid, 1)
          val count: Int = innerJedis.hget(key, elem.userid + ":" + elem.adid).toInt
          if (count >= 100) {
            //超过限制次数，添加到黑名单中
            innerJedis.sadd("blacklist", elem.userid)
          }
        }

        innerJedis.close()
      })
    })
    ssc.start()
    ssc.awaitTermination()

  }
}
