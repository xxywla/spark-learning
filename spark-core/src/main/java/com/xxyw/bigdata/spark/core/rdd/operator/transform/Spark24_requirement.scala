package com.xxyw.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_requirement {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // 1. 获取原始数据
        val dataRDD = sc.textFile("datas/agent.log")

        // 2. 将原始数据进行结构的转换
        val mapRDD = dataRDD.map(
            line => {
                val datas = line.split(" ")
                ((datas(1), datas(4)), 1)
            }
        )

        // 3. 分组聚合
        val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)

        // 4. 将聚合的结果进行结构的转换
        val mapRDD2 = reduceRDD.map {
            case ((prv, ad), sum) => {
                (prv, (ad, sum))
            }
        }

        // 5. 将转换结构后的数据根据省份进行分组
        val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD2.groupByKey()

        // 6. 将分组后的数据组内排序（降序），取前3名
        val resultRDD = groupRDD.mapValues(
            iter => {
                iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
            }
        )

        // 7. 采集数据打印在控制台
        resultRDD.collect().foreach(println)

        sc.stop()
    }

}
