package model.streaming.outputOperation

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.StreamingContext
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.Seconds

/**
 * spark streaming结果输出：
 *  1. 如果RDD的action操作，如果一个DStream及其派生出来的DStream都没有被执行输出操作，那么这些DStream不会被求值；
 * 2. DStream中每个批次的结果都被保存在给定目录的子目录中，且文件名包含指定的前缀和后缀。
 * 3. foreachRDD()，它用来对DStream中的RDD运行任意的函数。与transform类似。
 *
 */
object ForeachRDDDemo {
  def main(args: Array[String]): Unit = {
    val brokers = "192.168.6.83:9092,192.168.6.84:9092,192.168.6.89:9092"
    val topics = "T-USER-LOGINRECORD"
    // Create context with 2 second batch interval  
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // Create direct kafka stream with brokers and topics
//    val topicsSet = topics.split(",").toSet
//    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
//    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    val messages = ssc.socketTextStream("192.168.6.83", 9999)
    
    // Get the lines, split them into words, count the words and print 
    val wordCounts = messages.flatMap { x => x.split(" ") }.map(x => (x, 1)).reduceByKey(_+_)

    wordCounts.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        //ConnectionPool is a static, lazily initialized pool of connections
        lazy val connection = ConnectionPool.getConnection()
        //val sql ="insert into disc_mode_amt"+"(STATION_CODE,DISC_AMT)"+"values(?,?);"
        val sql ="insert into spark"+"values(word, 3);"
        partitionOfRecords.foreach(record => connection.createStatement().executeUpdate(sql))
        ConnectionPool.returnConnection(connection) //return to pool for future reuse
      }
    }
    
    ssc.start()   
    ssc.awaitTermination() 
  }
}















