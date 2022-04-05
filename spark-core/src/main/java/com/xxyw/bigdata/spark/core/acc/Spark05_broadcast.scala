package com.xxyw.bigdata.spark.core.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark05_broadcast {
    def main(args: Array[String]): Unit = {

        val sparConf: SparkConf = new SparkConf().setMaster("local").setAppName("TestSave")
        val sc = new SparkContext(sparConf)

        val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

        val mp: mutable.Map[String, Int] = mutable.Map[String, Int](("a", 4), ("b", 5), ("c", 6))

        val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(mp)
        rdd1.map {
            case (k, v) =>
                val v2: Int = bc.value.getOrElse(k, 0)
                (k, (v, v2))
        }.collect().foreach(println)

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
