package com.xxyw.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_req_Session_Top10 {
    def main(args: Array[String]): Unit = {
        val sparConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TestSave")
        val sc = new SparkContext(sparConf)

        //1.读取文件
        val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
        actionRDD.cache()

        val top10Cid: List[String] = getTop10Cid(actionRDD)
        val clickRDD: RDD[String] = actionRDD.filter(
            action => {
                val data: Array[String] = action.split("_")
                if (data(6) != "-1") {
                    top10Cid.contains(data(6))
                } else {
                    false
                }
            }
        )
        // ( (cid, sessionID) , 1 )
        val reduceRDD: RDD[((String, String), Int)] = clickRDD.map(
            action => {
                val data: Array[String] = action.split("_")
                ((data(6), data(2)), 1)
            }
        ).reduceByKey(_ + _)

        val groupRDD: RDD[(String, Iterable[(String, Int)])] = reduceRDD.map {
            case ((cid, sid), cnt) => (cid, (sid, cnt))
        }.groupByKey()
        groupRDD.mapValues(
            iter => {
                iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
            }
        ).collect().foreach(println)

        sc.stop()
    }

    def getTop10Cid(actionRDD: RDD[String]): List[String] = {
        actionRDD.flatMap(
            action => {
                val data: Array[String] = action.split("_")
                if (data(6) != "-1") {
                    List((data(6), (1, 0, 0)))
                } else if (data(8) != "null") {
                    data(8).split(",").map(cid => (cid, (0, 1, 0)))
                } else if (data(10) != "null") {
                    data(10).split(",").map(cid => (cid, (0, 0, 1)))
                } else {
                    Nil
                }
            }
        ).reduceByKey(
            (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3)
        ).sortBy(_._2, ascending = false).take(10).map(_._1).toList
    }
}
