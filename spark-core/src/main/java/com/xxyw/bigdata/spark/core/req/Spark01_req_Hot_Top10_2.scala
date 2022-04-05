package com.xxyw.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_req_Hot_Top10_2 {
    def main(args: Array[String]): Unit = {
        val sparConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TestSave")
        val sc = new SparkContext(sparConf)

        //1.读取文件
        val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
        actionRDD.cache()

        //2.统计点击数
        val clickRDD: RDD[String] = actionRDD.filter(
            action => {
                val data: Array[String] = action.split("_")
                data(6) != "-1"
            }
        )
        val click: RDD[(String, (Int, Int, Int))] = clickRDD.map {
            action => {
                val data: Array[String] = action.split("_")
                (data(6), 1)
            }
        }.reduceByKey(_ + _).map {
            case (cid, cnt) => (cid, (cnt, 0, 0))
        }

        //3.统计下单数
        val orderRDD: RDD[String] = actionRDD.filter(
            action => {
                val data: Array[String] = action.split("_")
                data(8) != "null"
            }
        )
        val order: RDD[(String, (Int, Int, Int))] = orderRDD.flatMap {
            action => {
                val data: Array[String] = action.split("_")
                data(8).split(",").map(id => (id, 1))
            }
        }.reduceByKey(_ + _).map {
            case (cid, cnt) => (cid, (0, cnt, 0))
        }

        //4.统计支付数
        val payRDD: RDD[String] = actionRDD.filter(
            action => {
                val data: Array[String] = action.split("_")
                data(10) != "null"
            }
        )
        val pay = payRDD.flatMap {
            action => {
                val data: Array[String] = action.split("_")
                data(10).split(",").map(id => (id, 1))
            }
        }.reduceByKey(_ + _).map {
            case (cid, cnt) => (cid, (0, 0, cnt))
        }

        //5.排序，取前10
        click.union(order).union(pay).reduceByKey(
            (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3)
        ).sortBy(_._2, ascending = false).take(10).foreach(println)

        //6.打印

        sc.stop()
    }
}
