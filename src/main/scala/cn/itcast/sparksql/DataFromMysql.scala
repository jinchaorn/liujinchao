package cn.itcast.sparksql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFromMysql {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("DataFromMysql").master("local[2]").getOrCreate()
    //通过sparkSession读取mysql表中数据生成的dataFrame
      //指定url
      val url="jdbc:mysql://hadoop111:3306/spark"
      //指定表名
      val table="iplocation"
      //指定配置信息
      val properties = new Properties()
      properties.setProperty("user","root")
      properties.setProperty("password","000000")
      val mysqlDF: DataFrame = spark.read.jdbc(url,table,properties)
      //打印schema
      mysqlDF.printSchema()
       mysqlDF.select("longitude").show()
      spark.stop()
  }
}
