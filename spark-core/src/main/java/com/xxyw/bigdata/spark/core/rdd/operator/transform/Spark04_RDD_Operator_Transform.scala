package com.xxyw.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4)))
        // TODO 算子 mapPartitions
        val flatRDD: RDD[Int] = rdd.flatMap(
            list => {
                list
            }
        )
        flatRDD.collect().foreach(println)

        sc.stop()
    }

}
