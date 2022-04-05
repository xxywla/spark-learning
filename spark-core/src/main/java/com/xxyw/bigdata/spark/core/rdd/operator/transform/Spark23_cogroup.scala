package com.xxyw.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark23_cogroup {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2)))
        val rdd2 = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6), ("c", 7)))

        // cogroup : connect + group 分组连接
        val cgRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
        cgRDD.collect().foreach(println)

        sc.stop()
    }

}
