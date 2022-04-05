package com.xxyw.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(6, 2, 4, 5, 3, 1), 2);

        val sortRDD: RDD[Int] = rdd.sortBy(num => num)
        sortRDD.saveAsTextFile("output")

        sc.stop()
    }

}
