import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamingSocket {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingSocket").setMaster("local[2]")

    //2、创建SparkContext对象
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")

    //3、构建StreamingContext对象 ,需要2个参数，第一个是sparkContext对象，第二个是批处理的时间间隔
    //Seconds(5)表示每隔5s处理上一个5s的数据
    val ssc = new StreamingContext(sc, Seconds(1))
    //接受socket的数据
     val socketTextStream = ssc.socketTextStream("hadoop110", 9999)
    //切分每一行,获取所有的单词
    val words = socketTextStream.flatMap(x => x.split(" "))
    //为每一单词计1
    val wordAndOne = words.map((_, 1))
    val reduceByKey = wordAndOne.reduceByKey(_ + _)
    reduceByKey.print()
    //开启计算
    ssc.start()
    ssc.awaitTermination()
  }

}
