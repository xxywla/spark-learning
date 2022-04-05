package com.xxyw.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark22_join {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
        val rdd2 = sc.makeRDD(List(("a", 4), ("b", 5)))

        val leftJoinRDD: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)
        leftJoinRDD.collect().foreach(println)

        rdd1.rightOuterJoin(rdd2).collect().foreach(println)


        sc.stop()
    }

}
