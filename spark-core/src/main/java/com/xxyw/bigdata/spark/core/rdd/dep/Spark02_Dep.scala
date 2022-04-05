package com.xxyw.bigdata.spark.core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Dep {
    def main(args: Array[String]): Unit = {
        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)

        val lines: RDD[String] = sc.textFile("datas/1.txt")
        println(lines.dependencies)
        println("*******************************")

        val words: RDD[String] = lines.flatMap(_.split(" "))
        println(words.dependencies)
        println("*******************************")

        val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))
        println(wordToOne.dependencies)
        println("*******************************")

        val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
        println(wordToSum.dependencies)
        println("*******************************")

        val array: Array[(String, Int)] = wordToSum.collect()
        array.foreach(println)

        sc.stop()
    }

}
