package cn.itcast.sparksql


import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Data2Mysql {
  case class Person(id:Int,name:String,age:Int)
  def main(args: Array[String]): Unit = {
    //1、创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().appName("Data2Mysql").getOrCreate()

    //2、获取SparkContext对象
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("warn")

    //3、读取数据文件
    val rdd1: RDD[String] = sc.textFile(args(0))

    //4、切分每一行
    val rdd2: RDD[Array[String]] = rdd1.map(_.split(","))

    //5、把rdd2与样例类Person进行关联
    val personRDD: RDD[Person] = rdd2.map(x=>Person(x(0).toInt,x(1),x(2).toInt))

    //6、将personRDD转换成DataFrame
    //手动导入隐式转换
    import spark.implicits._
    val personDF: DataFrame = personRDD.toDF

    //7、将personDF注册成一张表
    personDF.createTempView("user")

    //8、统计分析结果
    val result: DataFrame = spark.sql("select * from user where age >10")
    //9、把结果数据写入到mysql表中
    //指定url
    val url="jdbc:mysql://hadoop111:3306/spark"
    //指定表名
    val table="user1"
    //指定配置信息
    val properties=new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","000000")
      result.write.mode("append").jdbc(url,table,properties)
      spark.stop()
  }

}
