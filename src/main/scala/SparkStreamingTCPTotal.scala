import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamingTCPTotal {

  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] ={
    val newCount : Int = runningCount.getOrElse(0)+newValues.sum
      Some(newCount)
  }

  def main(args: Array[String]): Unit = {
    //配置sparkConf参数
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingTCPTotal").setMaster("local[2]")
    //构建sparkContext对象
    val sc: SparkContext = new SparkContext(sparkConf)
    //设置日志输出的级别
    sc.setLogLevel("WARN")
    //构建StreamingContext对象，每个批处理的时间间隔
    val scc: StreamingContext = new StreamingContext(sc, Seconds(5))
    //设置checkpoint路径，当前项目下有一个ck目录
    scc.checkpoint("./ck")
    //注册一个监听的IP地址和端口  用来收集数据
    val lines: ReceiverInputDStream[String] = scc.socketTextStream("hadoop110", 9999)
    //切分每一行记录
    val words: DStream[String] = lines.flatMap(_.split(" "))
    //每个单词记为1
    val wordAndOne: DStream[(String, Int)] = words.map((_, 1))
    //累计统计单词出现的次数
    val result: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunction)
    result.print()
    scc.start()
    scc.awaitTermination()


}
}
