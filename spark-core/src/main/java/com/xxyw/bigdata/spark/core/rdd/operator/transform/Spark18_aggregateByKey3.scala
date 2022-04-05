package com.xxyw.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_aggregateByKey3 {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(
            ("a", 1), ("a", 2), ("b", 3),
            ("b", 4), ("b", 5), ("a", 6)),
            2)
        val aggRDD: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
            (t, v) => {
                (t._1 + v, t._2 + 1)
            },
            (t1, t2) => {
                (t1._1 + t2._1, t1._2 + t2._2)
            }
        )
        aggRDD.mapValues({
            case (x, y) => {
                x / y
            }
        }).collect.foreach(println)

        sc.stop()
    }

}
