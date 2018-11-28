import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * sparkStreming流式处理接受socket数据，实现单词统计
  */
object SparkStreamingTCP {
  def main(args: Array[String]): Unit = {
          val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingTCP").setMaster("loal[2]")
          val sc = new SparkContext(sparkConf)
//          new StreamingContext(sc.Seconds(5))
  }
}
