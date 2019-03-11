//package com.wangyg.sparkmall.offline
//
//import java.sql.{Connection, DriverManager, PreparedStatement}
//import java.util.UUID
//
//import com.alibaba.fastjson.{JSON, JSONObject}
//import com.wangyg.sparkmall.common.xutil._
//import com.wangyg.sparkmall.comm.xbean._
//import org.apache.spark.SparkConf
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql._
//import org.apache.spark.util.AccumulatorV2
//
//import scala.collection.immutable
//import scala.collection.mutable.HashMap
//
//
///*
//    注意：1. 判断是否包含的cateid--注意各个数据结构的数据类型
// */
//object CategorySessionClickTop10 {
//  def main(args: Array[String]): Unit = {
//    //准备sparkconf
//    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("categoryTop10")
//    //获取session会话sparksession
//    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
//
//    //从读取数据到生成top10--通过sparksql方式查询数据--因为数据放在hive.database数据库中
//    var database = getValueFromResource("config", "hive.database")
//    //use数据库
//    spark.sql("use " + database)
//    //查询数据，获取数据 --小技巧，使用1=1防止出现添加条件而导致各种判断场景，动态添加条件场景
//    val sqlString = "select * from user_visit_action where 1=1"
//
//    //判断范围条件是否存在，并且合法
//    //先获取json对象
//    val strJason: String = getValueFromResource("condition", "condition.params.json")
//
//    val parseOject: JSONObject = JSON.parseObject(strJason)
//    //判断是否是空字符串
//    val stringBuilder = new StringBuilder(sqlString)
//    val startDate = parseOject.getString("startDate")
//    val endDate = parseOject.getString("endDate")
//
//    val sqlBuilder = new StringBuilder(sqlString)
//    if (isNotEmptyString(startDate)) {
//      sqlBuilder.append(" and action_time >= '").append(startDate).append("'")
//    }
//
//    if (isNotEmptyString(endDate)) {
//      sqlBuilder.append(" and action_time <= '").append(endDate).append("'")
//    }
//    //打印查看是否获取正确
//    println(parseOject.getString("endDate"))
//    //获取dataframe
//    val dataFrame: DataFrame = spark.sql(stringBuilder.toString())
//
//    //需要写入隐式转换
//    import spark.implicits._
//
//    //转换成dataset--再获取rdd--
//    val rdd: RDD[UserVisitAction] = dataFrame.as[UserVisitAction].rdd
//
//    //现在的数据类型 userVisitAction    目标数据类型CategoryCountInfo
//
//    //需要使用累加器方式统计数据
//    val accumulatorV = new CategoryCountAccumulatorV2()
//    spark.sparkContext.register(accumulatorV)
//
//    //当前数据时互斥的.若只有click 就没有其他--进行转换完成后，不需要再进行掐操作，所以使用foreach
//    rdd.foreach(xbean => {
//      //判断类型，根据进行不同的key的拼接
//      if (xbean.click_category_id != -1) {
//        accumulatorV.add(xbean.click_category_id + "_click")
//      } else if (xbean.order_category_ids != null) {
//        xbean.order_category_ids.split(",").foreach(t => {
//          accumulatorV.add(t + "_order")
//        })
//
//      } else if (xbean.pay_category_ids != null) {
//        xbean.pay_category_ids.split(",").foreach(t => {
//          accumulatorV.add(t + "_pay")
//        })
//      }
//    })
//    //获取结果数据
//    val statResult: HashMap[String, Long] = accumulatorV.value
//
//    //此时的数据是什么样子的？ hashmap[string.long] string 是id_方式
//    //将结果进行分组
//    val transMap: Map[String, HashMap[String, Long]] = statResult.groupBy {
//      case (key, _) => {
//        key.split("_")(0) // id_pay/order/click 根据前面的id 进行分割分组
//      }
//    }
//    //分组后进行封装成对象
//    //获取taskId
//    var taskId = UUID.randomUUID().toString //randomUUID--然后转成字符串
//    val infoes: immutable.Iterable[CategoryCountInfo] = transMap.map {
//      case (cid, map) => {
//        CategoryCountInfo(taskId,
//          cid, map.getOrElse(cid + "_click", 0L),
//          map.getOrElse(cid + "_order", 0L),
//          map.getOrElse(cid + "_pay", 0L)) //封装成CategoryCountInfo 对象
//      }
//    }
//    //然后转化成数组进行排序
//
//    //模式匹配
//    val infosResult: List[CategoryCountInfo] = infoes.toList.sortWith {
//      case (left, right) => {
//        if (left.clickCount > right.clickCount) {
//          true
//        } else if (left.clickCount == right.clickCount) {
//          if (left.orderCount > right.orderCount) {
//            true
//          } else if (left.orderCount == right.orderCount) {
//            left.payCount > right.payCount
//          } else {
//            false
//          }
//        }
//        else {
//          false
//        }
//      }
//    }.take(10)
//
//
//    // Top10 热门品类中 Top10 活跃 Session 统计
//    //将日志进行筛选，只保留含有前十名的历史log数据
//    val cateid: List[String] = infosResult.map(_.category_id)
//    val filterRDD: RDD[UserVisitAction] = rdd.filter(logBean => {
//      cateid.contains(logBean.click_category_id + "")
//    })
//
//    //将日志数据结构进行转换
//    //过滤后的RDD进行格式转化(click_category_id+_+session ,1)
//    val mapTransRDD: RDD[(String, Int)] = filterRDD.map(bean => {
//      (bean.click_category_id + "_" + bean.session_id, 1)
//    })
//    //将转换的结果数据进行聚合
//    val reduceRDD: RDD[(String, Int)] = mapTransRDD.reduceByKey(_ + _)
//    //将聚合后的结果转换格式
//    //模式匹配比较合适方便
//    val transRDD2: RDD[(String, (String, Int))] = reduceRDD.map {
//      case (str, sum) => {
//        val split: Array[String] = str.split("_")
//        (split(0), (split(1), sum))
//      }
//    }
//    //将数据进行分组
//    val groupRDD: RDD[(String, Iterable[(String, Int)])] = transRDD2.groupByKey()
//
//    //将分组后的数据可迭代 转换list 进行排序，取出前10个
//    val resultInfo: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(list => {
//      list.toList.sortWith {
//        case (left, right) => {
//          left._2 > right._2
//        }
//      }.take(10) //每组拿出钱10名
//    })
//
//    //将结果写入数据库中
//    //排序完成后，进行写入数据库中
//    val driverClass: String = getValueFromResource("config", "jdbc.driver.class")
//    Class.forName(driverClass)
//    val url = getValueFromResource("config", "jdbc.url")
//    val username = getValueFromResource("config", "jdbc.user")
//    val password = getValueFromResource("config", "jdbc.password")
//
//
//    resultInfo.foreachPartition(t => {
//      val conn: Connection = DriverManager.getConnection(url, username, password)
//      val insertSql = "insert into category_top10_session_count values (?, ?, ?, ?)"
//      val pst: PreparedStatement = conn.prepareStatement(insertSql)
//      t.foreach {
//        case (cid, datas) => {
//          datas.foreach(t => {
//            pst.setObject(1, taskId)
//            pst.setObject(2, cid)
//            pst.setObject(3, t._1)
//            pst.setObject(4, t._2)
//            pst.executeUpdate()
//          })
//        }
//      }
//    })
//
//    /*
//    datas => {
//    })
//     */
//    println("完成.....")
//  }
//
//  //累加器
//  //泛型--in out
//  class CategoryCountAccumulatorV2 extends AccumulatorV2[String, HashMap[String, Long]] {
//    private var map = new HashMap[String, Long]
//
//    override def isZero: Boolean = {
//      map.isEmpty
//    }
//
//    override def copy(): AccumulatorV2[String, HashMap[String, Long]] = {
//      //返回值类型是accumlatorV2所以直接创造一个新的即可
//      new CategoryCountAccumulatorV2();
//    }
//
//    //reset 清理工作
//    override def reset(): Unit = {
//      map = new HashMap[String, Long];
//    }
//
//    //分区内进行累加操作
//    override def add(v: String): Unit = {
//      //如果没有值默认值为0，然后再加1否则在原来基础上+1
//      map(v) = map.getOrElse(v, 0L) + 1L;
//    }
//
//    //分区间进行累加--分区间两个值
//    override def merge(other: AccumulatorV2[String, HashMap[String, Long]]): Unit = {
//      map = map.foldLeft(other.value) {
//        case (xmap, (key, value)) => {
//          xmap(key) = xmap.getOrElse(key, 0L) + value;
//          xmap
//        }
//      }
//    }
//
//    override def value: HashMap[String, Long] = map
//  }
//
//}
