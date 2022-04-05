package com.xxyw.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_join {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
        val rdd2 = sc.makeRDD(List(("b", 5), ("c", 6), ("a", 4)))

        val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
        joinRDD.collect().foreach(println)

        // 如果两个数据源中key没有匹配上，那么数据不会出现在结果中
        // 如果两个数据源中key有多个相同的，会依次匹配，可能会出现笛卡尔乘积，数据量会几何性增长，会导致性能降低

        sc.stop()
    }

}
