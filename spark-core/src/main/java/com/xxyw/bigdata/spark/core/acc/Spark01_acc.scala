package com.xxyw.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_acc {
    def main(args: Array[String]): Unit = {

        val sparConf: SparkConf = new SparkConf().setMaster("local").setAppName("TestSave")
        val sc = new SparkContext(sparConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        //        val i: Int = rdd.reduce(_ + _)
        //        println(i)
        var sum = 0
        rdd.foreach(num => {
            sum += num
        })
        println(sum)

        sc.stop()
    }

}
