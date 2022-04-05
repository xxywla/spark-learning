package com.xxyw.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {
    def main(args: Array[String]): Unit = {
        // TODO 准备环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 创建RDD
        // 从文件中创建RDD，将文件中的数据作为处理的数据源
        // path默认路径当前环境的根路径，可以写绝对路径，也可以写相对路径

//        val rdd: RDD[String] = sc.textFile("datas/1.txt")
        // path 路径可以是具体路径，也可以是目录
        val rdd: RDD[String] = sc.textFile("datas/*")

        // path 可以使用通配符

        // path 可以是分布式存储系统路径 HDFS

        rdd.collect().foreach(println)

        // TODO 关闭环境
        sc.stop()
    }
}
