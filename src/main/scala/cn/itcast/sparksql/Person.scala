package cn.itcast.sparksql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object CaseClassSchema {
  case class Person(id:Int,name:String,age:Int)
  def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("CaseClassSchema").master("local[2]").getOrCreate()
        //获取sparkContext对象
          val sc: SparkContext = spark.sparkContext
      sc.setLogLevel("warn")
        //读取文件
        val rdd1: RDD[String] = sc.textFile("E:\\person.txt")
        //切分每一行
          val rdd2: RDD[Array[String]] = rdd1.map(_.split(" "))
          val personRDD: RDD[Person] = rdd2.map(x=>Person(x(0).toInt,x(1),x(2).toInt))
          //将RDD转为DataFrame
          import spark.implicits._
            val personDF: DataFrame = personRDD.toDF()
            personDF.printSchema()
            personDF.show()
          //打印第一条信息
          println(personDF.first())
          //获取前三条信息
          personDF.head(3).foreach(println(_))
          personDF.select("name").show()
          personDF.select($"name").show()
          personDF.select(new Column("name")).show()
          //查询多个字段
            personDF.select("name","age").show()
            //过滤出年龄大于30的用户信息
          personDF.filter($"age">30).show()
          println(personDF.filter($"age">30).count)
          //按照age进行分组统计
          personDF.groupBy("age").count().show()
       //------------------------DSL风格语法-------------end
        //注册一张临时表
        personDF.createTempView("person")
        spark.sql("select*from person").show()
//        spark.sql("sleect name  from person where age>30 ").show()
        //关闭sparkSession
         spark.stop()
  }
}
