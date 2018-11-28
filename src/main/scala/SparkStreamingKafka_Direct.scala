import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamingKafka_Direct {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf = new SparkConf().setAppName("SparkStreamingKafka_Direct").setMaster("local[2]")
    //2.创建sparkContext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //3.创建sparkStreamingContext
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("./Kafka_Direct")
    //配置kafka相关的参数
    val kafkaParams = Map("metadata.broker.list" -> "hadoop110:9092,hadoop111:9092,hadoop112:9092", "group.id" -> "Kafka_Direct")
    //定义topic
    val topics = Set("kafka_spark")
    val directStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topics)
    //获取kafka中topic中的数据
    val topicData: DStream[String] = directStream.map(x => x._2)
    //切分每一行数据,单词后面加1
    val wordAndOne: DStream[(String, Int)] = topicData.flatMap(x => x.split(" ")).map((_, 1))
    //相同单词出现次数的累加
      val reduceByKey: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
      reduceByKey.print()
    //  开启计算
    ssc.start()
    //等待整个批次计算完成
    ssc.awaitTermination()
    ssc.awaitTermination()

  }
}
