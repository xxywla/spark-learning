package com.xxyw.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_Operator_Transform {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        // 第一个参数表示，抽取数据后是否将数据放回，true放回，false丢弃
        // 第二个参数表示，
        // 抽取不放回的场合，数据源中每条数据被抽取的概率
        // 基准值的概念，种子确定后每个值的概率都确定了，高于基准值的被抽取
        // 抽取放回的场合，表示数据源中每条数据被抽取的可能次数
        // 第三个参数表示，抽取数据时随机算法的种子，默认当前系统时间
        println(rdd.sample(
            true,
            0.4,
            1000003233
        ).collect().mkString(","))

        sc.stop()
    }

}
