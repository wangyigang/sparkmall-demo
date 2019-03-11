package com.wangyg.sparkmall.offline2

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.alibaba.fastjson.{JSON, JSONObject}
import com.wangyg.sparkmall.comm.xbean.UserVisitAction
import com.wangyg.sparkmall.common.xutil._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

// 页面单跳转化率统计
object PageOneJumpFlowTest2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("PageOneJumpFlowTest2")
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

    //根据获取到的数据进行处理
    //现货区目标页面id
    //获取统计目标的id
    val targetPageId: Array[String] = jSONObject.getString("targetPageFlow").split(",")
    println(targetPageId)

    // 进行groupby 进行分组按照 sessioin_id 分组
    //单跳转化率--使用session_id分组--统计每一个个人的，不同人之间的数据不进行统计

    //使用mapvalue方法统计每一个人的数据，因为已经进行了分组，不必关注key

    //将数据进行sortwith排序, 排序后，向要得到的数据是 （x->y ，1）形式，可是现在是对象形式
    //所以先要进行map 进行一次转换  只得到 pageid
    //然后进行zip 拉链操作， 将数据和后面的数据进行拼接 成(x->y)
    //然后在进行map =》 (x->y, 1)

    //然后得到的结果都在map的value中，所以通过Map 获取value, 然后进行flatmap扁平化
    //扁平化后将数据过滤，只获取我们关心的数据，所以，将目标id,进行  zip, 只满足target zip 后的数据，才获取

    //计算 分母的数据，将rdd进行过滤
    //然后map==>(x,1)  ==>reduce(_+_) ,然后

    //单挑转换率= （x->y）/x  x是当前页面的点击率，(x->y) 是x页面到y页面的点击次数，
    //    注意点：x->y 是必须是同一个人的 ， x->y 是要有先后顺序的
    //        数据要进行过滤，只保留想要查看页面的，可以提高效率
    // 分子要转换成（x->y，1） 进行统计综合， 分母要转换成x->y 要和分子进行匹配，否则过滤掉无用数据

    //将结果zip

    //获取到两个数据后，进行计算跳转率





  }
}
