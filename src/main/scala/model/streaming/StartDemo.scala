package model.streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

object StartDemo extends App{
  //1.创建一个有两个工作线程，批次间隔时间是1s的本地StreamingContext
  val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Milliseconds(1))
 
  //2.创建一个DStream,用来连接hostname:port
  //lines 代表了从data server上收到的数据流，在这个DStream上收到的每条记录都是一行text
  val lines = ssc.socketTextStream("127.0.0.1", 9999)
  
  //3.将每行数据切分成单词
  //flarMap操作将通过产生从每条记录中产生多个records来创建一个新的DStream,
  val words = lines.flatMap { x => x.split(" ") }
  
  //4.count each word in each batch
  val pairs = words.map { x => (x, 1) }
  val wordCounts = pairs.reduceByKey(_ + _)
  
  //5.print the first ten elements of each RDD generated in this DStream to the console
  //wordCounts.print()操作将每秒计算出来的结果打印到控制台
  wordCounts.print()
  
  //6.开始计算
  ssc.start()
  //7.等待计算结束
  ssc.awaitTermination()
  
  //KafkaUtils.createStream(jssc, zkQuorum, groupId, topics)
    
}