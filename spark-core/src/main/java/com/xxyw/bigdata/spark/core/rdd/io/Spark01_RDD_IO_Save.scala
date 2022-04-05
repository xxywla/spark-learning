package com.xxyw.bigdata.spark.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_IO_Save {
    def main(args: Array[String]): Unit = {
        val sparConf = new SparkConf().setMaster("local").setAppName("TestSave")
        val sc = new SparkContext(sparConf)

        val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

        rdd.saveAsTextFile("output1")
        rdd.saveAsObjectFile("output2")
        rdd.saveAsSequenceFile("output3")

        sc.stop()
    }
}
