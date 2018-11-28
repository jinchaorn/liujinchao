import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PV {
  def main(args: Array[String]): Unit = {
        //创建sparkConf对象,设置appName
        val sparkConf: SparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")
        val sc = new SparkContext(sparkConf)
        //读取文件
        val file: RDD[String] = sc.textFile(args(0))
        //统计次数
        val count: Long = file.count()
          println(count+"----------")
    sc.stop()
  }

}
