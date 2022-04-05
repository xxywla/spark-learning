package com.xxyw.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

object Spark06_RDD_Operator_Transform_Test {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.textFile("datas/apache.log")
        rdd.map(line => {
            val datas = line.split(" ")
            val time = datas(3)
            val sdf = new SimpleDateFormat("dd/MM/yy:HH:mm:ss")
            val date: Date = sdf.parse(time)
            val sdf2 = new SimpleDateFormat("HH")
            val hour: String = sdf2.format(date)
            (hour, 1)
        }).groupBy(_._1).map {
            case (hour, iter) => {
                (hour, iter.size)
            }
        }.collect().foreach(println)

        sc.stop()
    }
}
