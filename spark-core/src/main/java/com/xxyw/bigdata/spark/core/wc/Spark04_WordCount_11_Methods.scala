package com.xxyw.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_WordCount_11_Methods {
    def main(args: Array[String]): Unit = {
        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)

        wordCount11(sc)

        sc.stop()
    }

    // groupBy
    def wordCount1(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        words.groupBy(word => word).mapValues(iter => iter.size).collect().foreach(println)
    }

    // groupByKey
    def wordCount2(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val wordOne: RDD[(String, Int)] = words.map((_, 1))
        wordOne.groupByKey().mapValues(iter => iter.size).collect().foreach(println)
    }

    // reduceByKey
    def wordCount3(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val wordOne: RDD[(String, Int)] = words.map((_, 1))
        wordOne.reduceByKey(_ + _).collect().foreach(println)
    }

    // aggregateByKey
    def wordCount4(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val wordOne: RDD[(String, Int)] = words.map((_, 1))
        wordOne.aggregateByKey(0)(_ + _, _ + _).collect().foreach(println)
    }

    // foldByKey
    def wordCount5(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val wordOne: RDD[(String, Int)] = words.map((_, 1))
        wordOne.foldByKey(0)(_ + _).collect().foreach(println)
    }

    // combineByKey
    def wordCount6(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val wordOne: RDD[(String, Int)] = words.map((_, 1))
        wordOne.combineByKey(
            v => v,
            (x: Int, y) => x + y,
            (x: Int, y: Int) => x + y
        ).collect().foreach(println)
    }

    // countByKey
    def wordCount7(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val wordOne: RDD[(String, Int)] = words.map((_, 1))
        wordOne.countByKey().foreach(println)
    }

    // countByValue
    def wordCount8(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        words.countByValue().foreach(println)
    }

    // reduce
    def wordCount9(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val mapRDD: RDD[mutable.Map[String, Long]] = words.map(word => mutable.Map[String, Long]((word, 1)))
        val stringToLong: mutable.Map[String, Long] = mapRDD.reduce((mp1, mp2) => {
            mp2.foreach {
                case (word, count) => {
                    mp1.update(word, mp1.getOrElse(word, 0L) + count)
                }
            }
            mp1
        })
        println(stringToLong)
    }

    // aggregate
    def wordCount10(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val mapRDD: RDD[mutable.Map[String, Long]] = words.map(word => mutable.Map[String, Long]((word, 1)))
        val stringToLong: mutable.Map[String, Long] = mapRDD.aggregate(mutable.Map[String, Long]())((mp1, mp2) => {
            mp2.foreach {
                case (word, count) => {
                    mp1.update(word, mp1.getOrElse(word, 0L) + count)
                }
            }
            mp1
        }, (mp1, mp2) => {
            mp2.foreach {
                case (word, count) => {
                    mp1.update(word, mp1.getOrElse(word, 0L) + count)
                }
            }
            mp1
        })
        println(stringToLong)
    }

    // fold
    def wordCount11(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val mapRDD: RDD[mutable.Map[String, Long]] = words.map(word => mutable.Map[String, Long]((word, 1)))
        val stringToLong: mutable.Map[String, Long] = mapRDD.fold(mutable.Map[String, Long]())((mp1, mp2) => {
            mp2.foreach {
                case (word, count) => {
                    mp1.update(word, mp1.getOrElse(word, 0L) + count)
                }
            }
            mp1
        })
        println(stringToLong)
    }
}
