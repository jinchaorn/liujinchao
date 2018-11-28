import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamingSocketWindow {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingSocketWindow").setMaster("local[2]")

    //2、创建SparkContext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")

    //3、创建StreamingContext
    val ssc = new StreamingContext(sc,Seconds(2))

    //4、接受socket数据
    val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop110",9999)

    //5、切分每一行，获取所有的单词
    val words: DStream[String] = socketTextStream.flatMap(_.split(" "))
    //6、每个单词计为1
    val wordAndOne: DStream[(String, Int)] = words.map((_,1))

      val result: DStream[(String, Int)] = wordAndOne.reduceByKeyAndWindow((x:Int,y:Int)=>x+y,Seconds(10),Seconds(10))
    //8、打印
    result.print()

    //9、开启流式计算
    ssc.start()
    ssc.awaitTermination()

  }

}
