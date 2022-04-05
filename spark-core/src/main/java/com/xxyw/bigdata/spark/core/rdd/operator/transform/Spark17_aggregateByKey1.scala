package com.xxyw.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark17_aggregateByKey1 {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(
            ("a", 1), ("a", 2), ("b", 3),
            ("b", 4), ("b", 5), ("a", 6)),
            2)
        rdd.aggregateByKey(5)(
            (x, y) => math.max(x, y),
            (x, y) => x + y
        ).collect().foreach(println)

        sc.stop()
    }

}
