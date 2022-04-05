package com.xxyw.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File1 {
    def main(args: Array[String]): Unit = {
        // TODO 准备环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 创建RDD
        // 从文件中创建RDD，将文件中的数据作为处理的数据源

        // textFile以行为单位读取数据，读取数据是字符串
        // wholeTextFiles以文件为单位读取数据
        // 读取结果为元组，第一个元素表示文件路径，第二个元素表示文件内容
        val rdd: RDD[(String, String)] = sc.wholeTextFiles("datas/*")

        rdd.collect().foreach(println)

        // TODO 关闭环境
        sc.stop()
    }
}
