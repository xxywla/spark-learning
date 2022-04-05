package com.xxyw.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_persist {
    def main(args: Array[String]): Unit = {
        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)

        val list = List("Hello Scala", "Hello Spark")

        val rdd = sc.makeRDD(list)
        val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
        val mapRDD: RDD[(String, Int)] = flatRDD.map(word => {
            println("@@@@@")
            (word, 1)
        })
        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
        reduceRDD.collect().foreach(println)

        println("*********************")

        val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
        groupRDD.collect().foreach(println)

        sc.stop()
    }

}
