package com.xxyw.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark15_RDD_Operator_Transform {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))
        // 相同的key的数据进行value数据的聚合操作
        // Scala语言中一般的聚合操作都是两两聚合，spark基于Scala开发的，所以它的聚合也是两两聚合
        // 如果key的数据只有一个，是不会参与运算的
        val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey((x, y) => {
            println(s"x = ${x}, y = ${y}")
            x + y
        })
        reduceRDD.collect().foreach(println)

        sc.stop()
    }

}
