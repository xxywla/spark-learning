package com.xxyw.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {
    def main(args: Array[String]): Unit = {
        // 建立和Spark框架的连接
        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)

        // 执行业务操作

        val lines: RDD[String] = sc.textFile(path = "datas/*")

        val words: RDD[String] = lines.flatMap(_.split(" "))

        val wordToOne: RDD[(String, Int)] = words.map(
            word => (word, 1)
        )
        // Spark分组和聚合使用一个方法实现
        // 相同key的数据，可以对value进行reduce聚合
        val wordToCount = wordToOne.reduceByKey(_ + _)

        val array: Array[(String, Int)] = wordToCount.collect()
        array.foreach(println)

        // 关闭连接
        sc.stop()
    }
}
