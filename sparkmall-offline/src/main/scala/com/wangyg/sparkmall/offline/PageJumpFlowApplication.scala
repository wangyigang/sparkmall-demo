//package com.wangyg.sparkmall.offline
//
//import java.util.UUID
//
//import com.alibaba.fastjson.JSON
//import com.wangyg.sparkmall.common.xutil._
//import com.wangyg.sparkmall.comm.xbean._
//import org.apache.spark.SparkConf
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
//
//// 离线应用
//// 页面单跳转化率统计
///*
//    单挑转换率= （x->y）/x  x是当前页面的点击率，(x->y) 是x页面到y页面的点击次数，
//    注意点：x->y 是必须是同一个人的 ， x->y 是要有先后顺序的
//        数据要进行过滤，只保留想要查看页面的，可以提高效率
// */
//object PageJumpFlowApplication {
//
//    def main(args: Array[String]): Unit = {
//
//        // 创建Spark配置对象
//        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("CategoryTop10Application")
//        // 创建SparkSession对象
//        val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
//
//        // 导入隐式转换
//        import spark.implicits._
//
//        // 读取动作日志数据
//        spark.sql("use " + getConfigValue("hive.database"));
//
//        // 因为后续需要拼接动态条件，所以sql中增加where 1 = 1
//        val sql = "select * from user_visit_action where 1 = 1 "
//
//        // 获取查询条件
//        val jsonConfig : String = getValueFromResource("condition", "condition.params.json")
//
//        // 将JSON字符串转换为JSON对象
//        val jsonObject = JSON.parseObject(jsonConfig)
//
//        val startDate = jsonObject.getString("startDate")
//        val endDate = jsonObject.getString("endDate")
//
//        val sqlBuilder = new StringBuilder(sql)
//        if ( isNotEmptyString(startDate) ) {
//            sqlBuilder.append(" and action_time >= '").append(startDate).append("'")
//        }
//
//        if ( isNotEmptyString(endDate) ) {
//            sqlBuilder.append(" and action_time <= '").append(endDate).append("'")
//        }
//
//        // 查询数据，将数据转换为特定类型
//        val dataFrame: DataFrame = spark.sql(sqlBuilder.toString())
//
//        val actionLogRdd: RDD[UserVisitAction] = dataFrame.as[UserVisitAction].rdd
//
//        // 需求三的业务逻辑
//        // 4.1 将日志数据进行筛选过滤：符合跳转转换的页面ID（1~7）
//        //从配置文件中获取条件，过滤的条件，因为结果集是字符串类型并以 ， 进行拼接，所以
//        val pageids: Array[String] = jsonObject.getString("targetPageFlow").split(",")
//
//        // 4.2.1 将数据通过sessionid进行分组
//        // 进行groupby 进行分组按照 sessioin_id 分组
//        val sessionGroupRDD: RDD[(String, Iterable[UserVisitAction])] = actionLogRdd.groupBy(log=>log.session_id)
//        // 4.2.2 将过滤后的数据进行排序：按照点击时间升序
//        // [sessionid, List(UserVisitAction)]
//        // zip
//        // [sessionid, List(pageflow, count)]
//        // [List(pageflow, count)]
//        // [pageflow, count]
//        val zipLogRDD: RDD[(String, Int)] = sessionGroupRDD.mapValues(datas => {
//            val sortLogs: List[UserVisitAction] = datas.toList.sortWith {  //将数据转换成list,然后进行sortwith进行排序
//                case (left, right) => {
//                    left.action_time < right.action_time //按照点击时间进行排序
//                }
//            }
//            val sortPageids: List[Long] = sortLogs.map(log => log.page_id) //只获取log中的页ID数据，其他数据不关注
//            sortPageids.zip(sortPageids.tail).map { //进行zip拉链， 然后进行类型转换 数据中(x->y,1)格式
//                case (pageid1, pageid2) => {
//                    (pageid1 + "-" + pageid2, 1)
//                }
//            }
//        }).map(_._2).flatMap(x => x) //只关注value的数据，然后进行扁平化，去掉外部的数据类型
//
//        // 4.3 将筛选后的数据根据页面ID进行分组聚合统计点击次数（除数A）  //将数据进行过滤
//        val filterPageRDD: RDD[UserVisitAction] = actionLogRdd.filter(log => {
//            pageids.contains("" + log.page_id)
//        })
//        //map映射，(pageid , 1) => 进行汇总
//        val pageSumClickRDD: RDD[(Long, Long)] = filterPageRDD.map(log=>(log.page_id, 1L)).reduceByKey(_+_)
//        //然后将list转换成map形式，更利于后面的查找
//        val pageSumClickMap: Map[Long, Long] = pageSumClickRDD.collect().toMap
//
//        //将页面的转换ID进行拉链zip
//        // 4.4 将页面转换的ID进行拉链（zip）操作: (12, 23,34)
//        val zipPageArray: Array[String] = pageids.zip(pageids.tail).map {
//            case (pageid1, pageid2) => {
//                pageid1 + "-" + pageid2
//            }
//        }
//
//        // 4.5 将筛选后的日志数据进行拉链，然后和我们需要的页面转换拉链数据进行比对过滤
//        // 4.5.1 将数据进行过滤
//        val zipLogFilterRDD: RDD[(String, Int)] = zipLogRDD.filter(zipLog => {
//            zipPageArray.contains(zipLog._1)
//        })
//
//        // 4.6 将过滤的数据进行分组聚合统计点击次数（B）
//        // [pageflow, sum]
//        val zipLogReduceRDD: RDD[(String, Int)] = zipLogFilterRDD.reduceByKey(_+_)
//
//        val taskId = UUID.randomUUID().toString
//
//        // 4.7 将(A-B)/A结果保存到Mysql数据库
//        zipLogReduceRDD.foreach{
//            case (pageflow, sum) => {
//                val a = pageflow.split("-")(0)
//                val aCount = pageSumClickMap(a.toLong)
//
//                // 15 / 100 = 0.15564365456 * 100 = 15.45454545
//                val result = (sum.toDouble / aCount * 100).toLong
//                println(pageflow + ":" + result)
//            }
//        }
//
//
//
//
//// 操作数据库，保存数据
////        val driverClass = getConfigValue("jdbc.driver.class")
////        val url = getConfigValue("jdbc.url")
////        val user = getConfigValue("jdbc.user")
////        val password = getConfigValue("jdbc.password")
////
////        Class.forName(driverClass)
////        val conn : Connection = DriverManager.getConnection(url, user, password)
////
////        val insertSql = "insert into category_top10 values (?, ?, ?, ?, ?)"
////
////        val pstat: PreparedStatement = conn.prepareStatement(insertSql)
////
////        infoes.foreach(info=>{
////            pstat.setObject(1, info.taskid)
////            pstat.setObject(2, info.category_id)
////            pstat.setObject(3, info.clickCount)
////            pstat.setObject(4, info.orderCount)
////            pstat.setObject(5, info.payCount)
////            pstat.executeUpdate()
////        })
////
////        pstat.close()
////        conn.close()
//
//        spark.close()
//
//    }
//
//}
