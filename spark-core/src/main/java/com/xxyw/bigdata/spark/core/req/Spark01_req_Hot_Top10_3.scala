package com.xxyw.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_req_Hot_Top10_3 {
    def main(args: Array[String]): Unit = {
        val sparConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TestSave")
        val sc = new SparkContext(sparConf)

        //1.读取文件
        val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")

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
        ).sortBy(_._2, ascending = false).take(10).foreach(println)

        //6.打印

        sc.stop()
    }
}
