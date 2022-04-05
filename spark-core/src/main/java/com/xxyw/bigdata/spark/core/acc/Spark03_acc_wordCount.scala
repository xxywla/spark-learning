package com.xxyw.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark03_acc_wordCount {
    def main(args: Array[String]): Unit = {

        val sparConf: SparkConf = new SparkConf().setMaster("local").setAppName("TestSave")
        val sc = new SparkContext(sparConf)

        val rdd: RDD[String] = sc.makeRDD(List("hello", "spark", "hello"))

        val wcAcc = new MyAccumulator()
        sc.register(wcAcc, "wordCountAcc")
        rdd.foreach(word => {
            wcAcc.add(word)
        })
        println(wcAcc.value)

        sc.stop()
    }

    class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

        private var wcMap = mutable.Map[String, Long]()

        override def isZero: Boolean = {
            wcMap.isEmpty
        }

        override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
            new MyAccumulator()
        }

        override def reset(): Unit = {
            wcMap.clear()
        }

        override def add(v: String): Unit = {
            val newCount: Long = wcMap.getOrElse(v, 0L) + 1
            wcMap.update(v, newCount)
        }

        override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
            other.value.foreach {
                case (word, count) =>
                    val newCount: Long = wcMap.getOrElse(word, 0L) + count
                    wcMap.update(word, newCount)
            }
        }

        override def value: mutable.Map[String, Long] = {
            wcMap
        }
    }

}
