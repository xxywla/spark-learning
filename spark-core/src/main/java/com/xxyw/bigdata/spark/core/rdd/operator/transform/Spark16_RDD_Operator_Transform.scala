package com.xxyw.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_RDD_Operator_Transform {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))
        // 将数据源中的数据，相同key的数据分在一个组中，形成一个对偶元组
        // 元组中第一个元素就是key，第二个元素是相同key的value的集合
        val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()

        groupRDD.collect().foreach(println)

        val groupRDD1: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)

        sc.stop()
    }

}
