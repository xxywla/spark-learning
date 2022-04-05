package com.xxyw.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_RDD_Operator_Transform {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1, 2, 3, 4))

        val mapRDD = rdd.map((_, 1))
        // RDD => PairRDDFunctions
        // 隐式转换（二次编译）
        // 根据指定的分区规则对数据进行重分区
        mapRDD.partitionBy(new HashPartitioner(2))

        sc.stop()
    }

}
