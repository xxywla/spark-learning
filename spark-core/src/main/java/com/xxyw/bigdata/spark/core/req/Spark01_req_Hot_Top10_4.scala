package com.xxyw.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark01_req_Hot_Top10_4 {
    def main(args: Array[String]): Unit = {
        val sparConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TestSave")
        val sc = new SparkContext(sparConf)

        //1.读取文件
        val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")

        val hcAcc = new HotCategoryAcc()
        sc.register(hcAcc, "HotCategoryAcc")

        actionRDD.foreach(
            action => {
                val data: Array[String] = action.split("_")
                if (data(6) != "-1") {
                    hcAcc.add((data(6), "click"))
                } else if (data(8) != "null") {
                    data(8).split(",").foreach(cid => hcAcc.add((cid, "order")))
                } else if (data(10) != "null") {
                    data(10).split(",").foreach(cid => hcAcc.add(cid, "pay"))
                }
            }
        )
        hcAcc.value.values.toList.sortWith(
            (left, right) => {
                if (left.clickCnt > right.clickCnt) {
                    true
                } else if (left.clickCnt == right.clickCnt) {
                    if (left.orderCnt > right.orderCnt) {
                        true
                    } else if (left.orderCnt == right.orderCnt) {
                        left.payCnt > right.payCnt
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
        ).take(10).foreach(println)

        sc.stop()
    }

    case class HotCategory(cid: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int)

    class HotCategoryAcc extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {

        private val hcMap = mutable.Map[String, HotCategory]()

        override def isZero: Boolean = {
            hcMap.isEmpty
        }

        override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
            new HotCategoryAcc()
        }

        override def reset(): Unit = {
            hcMap.clear()
        }

        override def add(v: (String, String)): Unit = {
            val cid: String = v._1
            val action: String = v._2
            val hc: HotCategory = hcMap.getOrElse(cid, new HotCategory(cid, 0, 0, 0))
            if (action == "click") {
                hc.clickCnt += 1
            } else if (action == "order") {
                hc.orderCnt += 1
            } else if (action == "pay") {
                hc.payCnt += 1
            }
            hcMap.update(cid, hc)
        }

        override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
            other.value.foreach {
                case (cid, hc2) => {
                    val hc: HotCategory = hcMap.getOrElse(cid, new HotCategory(cid, 0, 0, 0))
                    hc.clickCnt += hc2.clickCnt
                    hc.orderCnt += hc2.orderCnt
                    hc.payCnt += hc2.payCnt
                    hcMap.update(cid, hc)
                }
            }
        }

        override def value: mutable.Map[String, HotCategory] = hcMap
    }
}
