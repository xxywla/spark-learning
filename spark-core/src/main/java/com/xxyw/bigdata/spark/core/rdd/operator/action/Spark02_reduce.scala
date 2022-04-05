package com.xxyw.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_reduce {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1, 2, 3, 4))

        val i: Int = rdd.reduce(_ + _)
        println(i)

        // collect方法会将不同分区的数据按照分区顺序采集到Driver端内存中，形成数组
        val ints: Array[Int] = rdd.collect()
        println(ints.mkString(","))

        val cnt: Long = rdd.count()
        println(cnt)

        val first: Int = rdd.first()
        println(first)

        val takeRes: Array[Int] = rdd.take(3)
        println(takeRes.mkString(","))

        val rdd1 = sc.makeRDD(List(4, 2, 3, 1))
        val takeOrderedRes: Array[Int] = rdd1.takeOrdered(3)
        println(takeOrderedRes.mkString(","))

        sc.stop()
    }
}
