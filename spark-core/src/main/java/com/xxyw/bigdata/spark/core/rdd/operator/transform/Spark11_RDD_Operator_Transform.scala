package com.xxyw.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Operator_Transform {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2);

        // 可以扩大分区，如果不进行shuffle操作，是没有意义的，不起作用
        //val newRDD: RDD[Int] = rdd.coalesce(3)
        val newRDD: RDD[Int] = rdd.repartition(3)
        newRDD.saveAsTextFile("output")
        sc.stop()
    }

}
