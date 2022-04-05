package com.xxyw.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Par {
    def main(args: Array[String]): Unit = {
        // TODO 准备环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 创建RDD
        // RDD的并行度 & 分区
        // makeRDD 传递第二个参数表示分区的数量
        // 第二个参数可以不传递，那么makeRDD会使用默认值
        //val rdd = sc.makeRDD(List(1,2,3,4),2)
        val rdd = sc.makeRDD(List(1, 2, 3, 4))
        // 将处理的数据保存为分区文件
        rdd.saveAsTextFile("output/")
        // TODO 关闭环境
        sc.stop()
    }
}
