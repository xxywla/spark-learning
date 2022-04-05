package com.xxyw.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_req_Hot_Top10 {
    def main(args: Array[String]): Unit = {
        val sparConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TestSave")
        val sc = new SparkContext(sparConf)

        //1.读取文件
        val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")

        //2.统计点击数
        val clickRDD: RDD[String] = actionRDD.filter(
            action => {
                val data: Array[String] = action.split("_")
                data(6) != "-1"
            }
        )
        val click: RDD[(String, Int)] = clickRDD.map {
            action => {
                val data: Array[String] = action.split("_")
                (data(6), 1)
            }
        }.reduceByKey(_ + _)

        //3.统计下单数
        val orderRDD: RDD[String] = actionRDD.filter(
            action => {
                val data: Array[String] = action.split("_")
                data(8) != "null"
            }
        )
        val order: RDD[(String, Int)] = orderRDD.flatMap {
            action => {
                val data: Array[String] = action.split("_")
                data(8).split(",").map(id => (id, 1))
            }
        }.reduceByKey(_ + _)

        //4.统计支付数
        val payRDD: RDD[String] = actionRDD.filter(
            action => {
                val data: Array[String] = action.split("_")
                data(10) != "null"
            }
        )
        val pay: RDD[(String, Int)] = payRDD.flatMap {
            action => {
                val data: Array[String] = action.split("_")
                data(10).split(",").map(id => (id, 1))
            }
        }.reduceByKey(_ + _)

        //5.排序，取前10
        val mergeRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = click.cogroup(order, pay)
        mergeRDD.mapValues {
            case (iter1, iter2, iter3) => {
                var cnt1 = 0
                val it1: Iterator[Int] = iter1.iterator
                if (it1.hasNext) {
                    cnt1 = it1.next()
                }
                var cnt2 = 0
                val it2: Iterator[Int] = iter2.iterator
                if (it2.hasNext) {
                    cnt2 = it2.next()
                }
                var cnt3 = 0
                val it3: Iterator[Int] = iter3.iterator
                if (it3.hasNext) {
                    cnt3 = it3.next()
                }
                (cnt1, cnt2, cnt3)
            }
        }.sortBy(_._2, false).take(10).foreach(println)

        //6.打印

        sc.stop()
    }
}
