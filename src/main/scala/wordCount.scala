package com.itcast.hellospark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object wordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    //设置日志输出的级别
    sc.setLogLevel("warn")
    //读取文件内容
    val file: RDD[String] = sc.textFile(args(0))
    // println(file.toString())
    //读取文件的每一行数据,然后进行切割,压扁
    val flatMap: RDD[String] = file.flatMap(_.split(" "))
    //然后将每个元素放入元组中,
    val map: RDD[(String, Int)] = flatMap.map((_,1))
    //将每个元组中的相同key的第二个元素累加
    val reduceByKey: RDD[(String, Int)] = map.reduceByKey(_+_)
    //按照降序排序输出
    val sortBy: RDD[(String, Int)] = reduceByKey.sortBy(_._2,false)
    //输出
    val array: Array[(String, Int)] = sortBy.collect()
    array.foreach(x=>println(x))
  }
}

