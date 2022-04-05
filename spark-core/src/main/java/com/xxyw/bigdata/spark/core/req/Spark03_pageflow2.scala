package com.xxyw.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_pageflow2 {
    def main(args: Array[String]): Unit = {
        val sparConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TestSave")
        val sc = new SparkContext(sparConf)

        //1.读取文件
        val actionRDD: RDD[UserVisitAction] = sc.textFile("datas/user_visit_action.txt").map(
            action => {
                val data: Array[String] = action.split("_")
                UserVisitAction(data(0), data(1).toLong, data(2), data(3).toLong, data(4),
                    data(5), data(6).toLong, data(7).toLong, data(8), data(9),
                    data(10), data(11), data(12).toLong)
            }
        )
        actionRDD.cache()

        // 对指定的页面连续跳转进行统计
        val ids: List[Long] = List[Long](1, 2, 3, 4, 5, 6, 7)

        val okIdPairs: List[(Long, Long)] = ids.zip(ids.tail)

        // TODO 计算分母
        val pageMap: Map[Long, Long] = actionRDD.filter(
            action => {
                ids.init.contains(action.page_id)
            }
        ).map(
            action => (action.page_id, 1L)
        ).reduceByKey(_ + _).collect().toMap

        // TODO 计算分子
        val groupRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)

        val idPairRDD: RDD[(Long, Long)] = groupRDD.values.flatMap {
            iter => {
                val pageIds: List[Long] = iter.toList.sortBy(_.date).map(action => action.page_id)
                val idPairs: List[(Long, Long)] = pageIds.zip(pageIds.tail)
                idPairs
            }
        }.filter(
            idPair => {
                okIdPairs.contains(idPair)
            }
        )
        val reduceRDD: RDD[((Long, Long), Int)] = idPairRDD.map(pair => (pair, 1)).reduceByKey(_ + _)
        reduceRDD.foreach {
            case ((from, to), cnt) =>
                val sum: Long = pageMap.getOrElse(from, 0L)
                println(s"从页面${from}到页面${to}的跳转概率为${cnt.toDouble / sum}")
        }
    }

    //用户访问动作表
    case class UserVisitAction(
                                date: String, //用户点击行为的日期
                                user_id: Long, //用户的 ID
                                session_id: String, //Session 的 ID
                                page_id: Long, //某个页面的 ID
                                action_time: String, //动作的时间点
                                search_keyword: String, //用户搜索的关键词
                                click_category_id: Long, //某一个商品品类的 ID
                                click_product_id: Long, //某一个商品的 ID
                                order_category_ids: String, //一次订单中所有品类的 ID 集合
                                order_product_ids: String, //一次订单中所有商品的 ID 集合
                                pay_category_ids: String, //一次支付中所有品类的 ID 集合
                                pay_product_ids: String, //一次支付中所有商品的 ID 集合
                                city_id: Long //城市 id
                              )
}
