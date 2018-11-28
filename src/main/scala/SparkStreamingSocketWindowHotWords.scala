import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamingSocketWindowHotWords {
  def main(args: Array[String]): Unit = {
    //1、创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingSocketWindowHotWords").setMaster("local[2]")

    //2、创建SparkContext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")

    //3、创建StreamingContext
    val ssc = new StreamingContext(sc,Seconds(1))

    //4、接受socket数据
    val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop110",9999)

    //5、切分每一行，获取所有的单词
    val words: DStream[String] = socketTextStream.flatMap(_.split(" "))

    //6、每个单词计为1
    val wordAndOne: DStream[(String, Int)] = words.map((_,1))
    //7、相同单词出现的1累加
    //reduceByKeyAndWindow 需要3个参数
    //第一个：就是函数
    //第二个：表示窗口的长度
    //第三个：表示窗口的滑动时间间隔，每隔多久计算一次
    val result: DStream[(String, Int)] = wordAndOne.reduceByKeyAndWindow((x:Int,y:Int)=>x+y,Seconds(10),Seconds(10))
    val sortResult: DStream[(String, Int)]  = result.transform(rdd=>{
          //按照单词出现次数降序排列
          val sortRDD: RDD[(String, Int)] = rdd.sortBy(_._2,false)
            val top3: Array[(String, Int)] = sortRDD.take(3)
          //打印
          println("-------------------top3----------------start")
          top3.foreach(println)
          println("-------------------top3----------------end")
          sortRDD
        })
    //8、打印
    sortResult.print()

    //9、开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }
}
