package cn.itcast.sparksql

import org.apache.spark.sql.SparkSession

object HiveSupport {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("HiveSupport").master("local[2]").enableHiveSupport().getOrCreate()
    //通过sparkSession操作hivesql
      //创建一张hive表
    // spark.sql("create table user(id int ,name string,age int ) row format delimited fields terminated by ','")
      //向表中加载数据
      spark.sql("load data  local inpath './person.txt' into table user")
    //查询表数据
      spark.sql("select * from user").show()
      spark.stop()
  }
}
