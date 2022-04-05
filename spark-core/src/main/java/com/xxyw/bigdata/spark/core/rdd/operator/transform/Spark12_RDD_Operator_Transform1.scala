package com.xxyw.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform1 {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), 2);
        // 根据指定的规则对数据源中的数据进行排序，默认为升序，第二个参数可以改变排序的方式
        // 不会改变分区，但存在shuffle操作
        val sortRDD = rdd.sortBy(t => t._1.toInt, false)
        sortRDD.collect().foreach(println)

        sc.stop()
    }

}
