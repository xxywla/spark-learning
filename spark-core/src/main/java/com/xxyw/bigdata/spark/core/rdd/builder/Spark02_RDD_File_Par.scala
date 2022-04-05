package com.xxyw.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File_Par {
    def main(args: Array[String]): Unit = {
        // TODO 准备环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 创建RDD
        // textFile 将文件作为数据处理的数据源，默认也可以设定分区
        // minPartitions 最小分区数量
        // math.min(defaultParallelism, 2)
        val rdd = sc.textFile("datas/1.txt")


        // TODO 关闭环境
        sc.stop()
    }
}
