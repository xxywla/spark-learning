package com.xxyw.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Transform {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 mapPartitions
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

        // 可以以分区为单位进行数据转换操作 整个分区数据加载内存中 处理完数据不会被释放 存在对象印象
        // 内存较小 数据量较大 内存溢出
        val mpRDD: RDD[Int] = rdd.mapPartitions(
            iter => {
                println(">>>>>>>")
                iter.map(_ * 2)
            }
        )
        mpRDD.collect().foreach(println)

        sc.stop()
    }

}
