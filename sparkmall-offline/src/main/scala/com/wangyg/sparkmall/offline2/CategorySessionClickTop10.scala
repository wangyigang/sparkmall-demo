package com.wangyg.sparkmall.offline2

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.alibaba.fastjson.{JSON, JSONObject}
import com.wangyg.sparkmall.common.xutil._
import com.wangyg.sparkmall.comm.xbean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.{immutable, mutable}

/*
    Top10 热门品类中 Top10 活跃 Session 统计
 */
object CategorySessionClickTop10 {
  def main(args: Array[String]): Unit = {
    //创建sparkconf
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("categoryTop10")
    //创建sparksession--创建sparksession能够支持hive
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //支持隐式转换
    import spark.implicits._

    //读取hive中的原始数据文件，进行分析
    //先使用对应的数据库--进行切换数据库
    val hivedb = getConfigValue("hive.database")
    spark.sql("use " + hivedb)

    //进行查询 --where 1=1 小技巧，避免很多的逻辑判断
    var sqlStr = "select * from user_visit_action where 1=1"


    //获取上线条件
    val str: String = getValueFromResource("condition", "condition.params.json")
    val jSONObject: JSONObject = JSON.parseObject(str)
    val startDate: String = jSONObject.getString("startDate")

    //创建stringbuder进行拼接--更加灵活
    val builder = new StringBuilder(sqlStr)
    if (isNotEmptyString(startDate)) {
      builder.append(" and action_time >= '").append(startDate).append("'")
    }

    //判断下行条件
    val endDate: String = jSONObject.getString("endDate")
    if (isNotEmptyString(endDate)) {
      //将调价进行拼接
      builder.append(" and action_time <= '").append(endDate).append("'")
    }

    val dataFrame: DataFrame = spark.sql(builder.toString())


    //将dataFrame转换成dataset，然后获取对应的rdd
    val rdd: RDD[UserVisitAction] = dataFrame.as[UserVisitAction].rdd


    //此时初始数据是rdd, 希望得到的结果是 CategoryCountInfo样例类
    //1.使用累加器方式对数据进行处理，使用join方式效率太低
    val accumulator = new CategoryCountAccumulator()
    //进行注册
    spark.sparkContext.register(accumulator)

    //统计思想：
    rdd.foreach(bean => {
      //根据bean对象的数据进行判断
      if (bean.click_category_id != -1) {
        accumulator.add(bean.click_category_id + "_click")
      } else if (bean.order_category_ids != null) {
        bean.order_category_ids.split(",").foreach(t => {
          accumulator.add(t + "_order")
        })
      } else if (bean.pay_category_ids != null) {
        bean.pay_category_ids.split(",").foreach(t => {
          accumulator.add(t + "_pay")
        })
      }
    })
    //累加器返回的结果
    val accResult: mutable.HashMap[String, Long] = accumulator.value
    //此时累加器的数据时一个 进行拼接成，然后分区
    // 疑问，是先分区还是先拼接
    val map: Map[String, mutable.HashMap[String, Long]] = accResult.groupBy {
      case (key, sum) => {
        key.split("_")(0) //以_进行切分，使用前面部分进行分组
      }
    }
    val taskId: String = UUID.randomUUID().toString
    val infoes: immutable.Iterable[CategoryCountInfo] = map.map {
      case (cid, hsmap) => {
        CategoryCountInfo(taskId, cid,
          hsmap.getOrElse(cid + "_click", 0L),
          hsmap.getOrElse(cid + "_order", 0L),
          hsmap.getOrElse(cid + "_pay", 0L))
      }
    }
    println(infoes.size)

    //排序
    val infoesResult: List[CategoryCountInfo] = infoes.toList.sortWith {
      case (left, right) => {
        if (left.clickCount > right.clickCount) {
          true
        } else if (left.clickCount == right.clickCount) {
          if (left.orderCount > right.orderCount) {
            true
          } else if (left.orderCount == right.orderCount) {
            left.payCount > right.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    }.take(10) //取出前十名
    println(infoesResult.size)

    //需求二：
    //Top10 热门品类中 Top10 活跃 Session 统计
    val listCID: List[String] = infoesResult.map(_.category_id)
    val filterRDD = rdd.filter(bean => {
      listCID.contains(bean.click_category_id + "")
    })

    //过滤掉数据后，// categoryid session sum三个变量
    //过滤掉无效数据后，进行格式转换

    val mapRDD: RDD[(String, Int)] = filterRDD.map {
      case key => {}
        (key.click_category_id + "_" + key.session_id, 1)
    }
    //进行聚合
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    //在进行转化
    val mapRDD2: RDD[(String, (String, Int))] = reduceRDD.map {
      case (str, sum) => {
        (str.split("_")(0), (str.split("_")(1), sum))
      }
    }
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD2.groupByKey()


    //    将分组后的数据可迭代 转换list 进行排序，取出前10个
    val resultInfo: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(list => {
      list.toList.sortWith {
        case (left, right) => {
          left._2 > right._2
        }
      }.take(10) //每组拿出钱10名
    })

    //将结果写入数据库中
    //排序完成后，进行写入数据库中
    val driverClass: String = getValueFromResource("config", "jdbc.driver.class")
    Class.forName(driverClass)
    val url = getValueFromResource("config", "jdbc.url")
    val username = getValueFromResource("config", "jdbc.user")
    val password = getValueFromResource("config", "jdbc.password")


    resultInfo.foreachPartition(t => {
      val conn: Connection = DriverManager.getConnection(url, username, password)
      val insertSql = "insert into category_top10_session_count values (?, ?, ?, ?)"
      val pst: PreparedStatement = conn.prepareStatement(insertSql)
      t.foreach {
        case (cid, datas) => {
          datas.foreach(t => {
            pst.setObject(1, taskId)
            pst.setObject(2, cid)
            pst.setObject(3, t._1)
            pst.setObject(4, t._2)
            pst.executeUpdate()
          })
        }
      }
    })


    println("完成.....")

  }

  //定义累加器--进行合并数据
  class CategoryCountAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {
    private var map = new mutable.HashMap[String, Long]()

    /**
      * 判断是否是0值
      *
      * @return
      */
    override def isZero: Boolean = {
      map.isEmpty
    }

    /**
      * 复制
      *
      * @return
      */
    override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
      new CategoryCountAccumulator()
    }

    override def reset(): Unit = {
      new mutable.HashMap[String, Long]()
    }

    /**
      * 分区内累加 --初始值为0+1l
      *
      * @param v
      */
    override def add(v: String): Unit = {
      map(v) = map.getOrElse(v, 0L) + 1L
    }

    /**
      * 分区间算法--两个
      *
      * @param other
      */
    override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
      map = map.foldLeft(other.value) {
        case (xmap, (key, sum)) => {
          xmap(key) = xmap(key) + sum
          xmap
        }
      }
    }

    override def value: mutable.HashMap[String, Long] = map
  }

}

