package cn.itcast.sparksql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object StructTypeSchema {
  def main(args: Array[String]): Unit = {
    //1、创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().appName("StructTypeSchema").master("local[2]").getOrCreate()
    //2、创建SparkContext对象
      val sc: SparkContext = spark.sparkContext
      sc.setLogLevel("warn")
    //3、读取数据文件
    val data: RDD[String] = sc.textFile("E:\\person - 副本.txt")
      print("---------"+data.collect().toBuffer)

    //4、切分每一行
    val rdd1: RDD[Array[String]] = data.map(_.split(" "))
    rdd1.foreach(print(_))
      val rowRDD: RDD[Row] = rdd1.map(x=>Row(x(0).toInt,x(1),x(2).toInt))
    val schema = StructType(
        StructField("id", IntegerType, true) ::
        StructField("name", StringType, false) ::
        StructField("age", IntegerType, false) :: Nil)
      val dataFrame: DataFrame = spark.createDataFrame(rowRDD,schema )
      dataFrame.printSchema()
      //显示数据
      //  dataFrame.show()
        //建一张临时表
        dataFrame.createTempView("person")
        spark.sql("select*from person order by age desc").show()
        //关闭
        spark.stop()
  }
}
