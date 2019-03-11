package com.wangyg.sparkmall.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.wangyg.sparkmall.common.xutil._
import com.wangyg.sparkmall.comm.xbean.UserVisitAction
import org.apache.spark.rdd.RDD

/*
    注意点：过滤时的比较条件注意两个数据的类型 targetPageId.contains(t.page_id+"") 防止类型不同
 */

// 离线应用
// 页面单跳转化率统计
object PageOneJumpFlowTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("PageOneJumpFlowTest")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    //导入隐式转换
    import spark.implicits._
    //使用spark sql进行查询到数据，封装为rdd类型
    //获取hive数据库名称
    val dbname: String = getConfigValue("hive.database")
    //使用数据库
    spark.sql("use " + dbname)

    var sql = "select * from user_visit_action where 1=1"
    //获取条件
    val jsonStr: String = getValueFromResource("condition", "condition.params.json")
    val jSONObject: JSONObject = JSON.parseObject(jsonStr)
    val startDate: String = jSONObject.getString("startDate")
    //判断条件是否满足
    val builder = new StringBuilder(sql)
    if (isNotEmptyString(startDate)) {
      //添加数据
      builder.append(" and action_time >= '").append(startDate).append("'")
    }
    val endDate: String = jSONObject.getString("endDate")
    if (isNotEmptyString(endDate)) {
      builder.append(" and action_time <= '").append(endDate).append("'")
    }
    val dataFrame: DataFrame = spark.sql(builder.toString())
    //获取到原始数据
    val rdd: RDD[UserVisitAction] = dataFrame.as[UserVisitAction].rdd

    //根据需求进行处理
    // 页面单跳转化率统计--获取总共的每个页面的点击率， 获取(x->y)的点击率，然后相除 统计每一个人的s

    //获取统计目标的id
    val targetPageId: Array[String] = jSONObject.getString("targetPageFlow").split(",")
    println(targetPageId)

    // 进行groupby 进行分组按照 sessioin_id 分组
    val groupRDD: RDD[(String, Iterable[UserVisitAction])] = rdd.groupBy(log => {
      log.session_id
    })
    val zipLogRDD: RDD[(String, Int)] = groupRDD.mapValues {
      case datas => {
        val sortedList: List[UserVisitAction] = datas.toList.sortWith {
          case (left, right) => {
            left.action_time < right.action_time //按照点击时间进行排序
          }
        }
        val pageidList: List[Long] = sortedList.map(bean => {
          bean.page_id //只关注页面id
        })
        //两个进行zip拉链合并--合并后，将结果转为map结构 --得到的结果为: 此时得到的结果是(x,y) 但是要转换结果
        pageidList.zip(pageidList.tail).toMap.map {
          case (first, second) => {
            (first + "-" + second, 1)
          }
        }
      }
    }.map(_._2).flatMap(x => x)


    //将结果zip
    val allTargetPage: Array[String] = targetPageId.zip(targetPageId.tail).map {
      case (first, second) => {
        first + "-" + second
      }
    }

    val ziplogFilterRDD: RDD[(String, Int)] = zipLogRDD.filter(ziplog => {
      allTargetPage.contains(ziplog._1)
    }).reduceByKey(_ + _)


    //单挑转换率= （x->y）/x  x是当前页面的点击率，(x->y) 是x页面到y页面的点击次数，
    //    注意点：x->y 是必须是同一个人的 ， x->y 是要有先后顺序的
    //        数据要进行过滤，只保留想要查看页面的，可以提高效率
    // 分子要转换成（x->y，1） 进行统计综合， 分母要转换成x->y 要和分子进行匹配，否则过滤掉无用数据

    val filterRDD: RDD[UserVisitAction] = rdd.filter(t => {
      targetPageId.contains(t.page_id + "") //注意点:两个数据的类型
    })

    val pageSumClickMap: Map[Long, Long] = filterRDD.map(log => (log.page_id, 1L))
        .reduceByKey(_ + _).collect().toMap



    //获取到两个数据后，进行计算跳转率
    val resultRDD = ziplogFilterRDD.map {
      case (key, sum) => {
        val first: String = key.split("-")(0)
        val rate = (sum.toDouble / pageSumClickMap(first.toLong) * 100)
        println(key + ":" + rate)
        (key, rate)
      }
    }

    val driverClass = getConfigValue("jdbc.driver.class")
    val url = getConfigValue("jdbc.url")
    val user = getConfigValue("jdbc.user")
    val password = getConfigValue("jdbc.password")

    Class.forName(driverClass)


    val taskid: String = UUID.randomUUID().toString
    resultRDD.foreach(info => {
      val conn: Connection = DriverManager.getConnection(url, user, password)

      val insertSql = "insert into page_jump_rate values (?, ?, ?)"

      val pstat: PreparedStatement = conn.prepareStatement(insertSql)

      pstat.setObject(1, taskid)
      pstat.setObject(2, info._1)
      pstat.setObject(3, info._2)
      pstat.executeUpdate()

      pstat.close()
      conn.close()
    })

    spark.close()
  }
}
