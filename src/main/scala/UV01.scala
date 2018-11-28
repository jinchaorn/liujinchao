import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object UV01 {
  def main(args: Array[String]): Unit = {
    //创建sparkConf对象,设置appName
    val sparkConf: SparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    //读取文件
    val file: RDD[String] = sc.textFile(args(0))
    val distinct: RDD[String] = file.map(x=>x.split(" ")(0)).distinct()
    val count: Long = distinct.count()
    println(count)
  }

}
