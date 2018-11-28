import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Iplocation {

  def binarySearch(ipNum: Long, broadCastValue: Array[(String, String, String, String)]):Int ={
      var start=0
      var end=broadCastValue.length-1
        while (start<=end){
            var middle=(start+end)/2
            if(ipNum<=broadCastValue(middle)._2.toLong&&ipNum>=broadCastValue(middle)._1.toLong){
              return  middle
            }
          if(ipNum>broadCastValue(middle)._2.toLong){
              start=middle
          }
          if(ipNum<broadCastValue(middle)._1.toLong){
            end=middle
          }
        }
          -1
  }



           def wirtemysql(iter:Iterator[((String, String), Int)]):Unit ={
             //定义数据库连接
           var conn : Connection=null
          //定义PreparedStatement
          var ps: PreparedStatement=null
          //定义sql语句
          val sql = "insert into iplocation(longitude,latitude,total_count)values(?,?,?)"
          //获取数据库的连接
          conn= DriverManager.getConnection("jdbc:mysql://hadoop111:3306/spark","root","000000")
          ps= conn.prepareStatement(sql)
             try{
              iter.foreach(line=>{
                  ps.setString(1,line._1._1)
                  ps.setString(2,line._1._2)
                  ps.setLong(3,line._2)
                  ps.execute()
              })
             }catch {
               case e:Exception => println(e)
             }
             finally {
               if(ps!=null){
                 ps.close()
               }
               if(conn!=null){
                 conn.close()
               }
             }

        }

  def main(args: Array[String]): Unit = {
    //创建sparkConf对象,设置appName
    val sparkConf: SparkConf = new SparkConf().setAppName("Iplocation").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    //读取城市ip段信息文件
      val map: RDD[Array[String]] = sc.textFile(args(0)).map(x=>x.split("\\|"))
      val city_ip_rdd: RDD[(String, String, String, String)] = map.map((x=>(x(2),x(3),x(x.length-2),x(x.length-1))))
    //将城市id广播到每一个worker
      val broadcast: Broadcast[Array[(String, String, String, String)]] = sc.broadcast(city_ip_rdd.collect())
    //读取文件
    val file: RDD[String] = sc.textFile(args(1))
    val ips: RDD[String] = file.map(x=>x.split("\\|")(1))
    //遍历ip地址,去匹配对应的区间
    val result: RDD[((String, String), Int)] =  ips.mapPartitions(iter=>{
          //获取广播变量的值
            val broadCastValue: Array[(String, String, String, String)] = broadcast.value
            //遍历迭代器
            iter.map(ip=>{
              //把ip地址转换为long类型
              val ipNum: Long = ipToLong(ip)
              //通过二分法去匹配ip地址在数组中的下标
              val index: Int = binarySearch(ipNum,broadCastValue)
              //定位到经纬度
              val destValue: (String, String, String, String) = broadCastValue(index)
              //把结果封装成((经度,纬度),1)
              ((destValue._3,destValue._4),1)
            })
      })
      val finalResult: RDD[((String, String), Int)] = result.reduceByKey(_+_)
        //打印输出
        finalResult.foreach(print(_))
      //将结果数据写入到mysql中
        finalResult.foreachPartition(wirtemysql)

  }

  def ipToLong(ip:String) ={
      val ips:Array[String]=ip.split("\\.")
      var ipNum:Long=0L
    for(i <- ips){
      ipNum=i.toLong  |  ipNum << 8L
    }
    ipNum
  }

}
