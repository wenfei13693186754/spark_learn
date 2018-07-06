package model.streaming.streaming_kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import org.apache.spark.rdd.RDD

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: DirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
 *    topic1,topic2
 */
object DirectKafkaWordCount {
  var sparkConf: SparkConf = null
  var ssc: StreamingContext = null
  val checkpointDirectory = "hdfs://192.168.6.45:9000/ptyhzx_online_statistics/directTest4"
  def main(args: Array[String]) {

    val brokers = "192.168.6.83:9092,192.168.6.84:9092,192.168.6.89:9092"
    //val topics = "TEST_LOGINLOG"
    val topics = "T-USER-LOGINRECORD"
    // Create context with 2 second batch interval 
    sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[*]")
    ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("hdfs://192.168.6.45:9000/ptyhzx_online_statistics/directTest4")
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "largest")
    var count = 0L
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    var num = 0
    messages.foreachRDD {   rdd: RDD[(String, String)] =>
      rdd.foreach { x =>
        //var obj: JSONObject = JSON.parseObject(x._2)
        println("消费到数据" + x._2)
      }
      num = num + 1
      count = rdd.count() + count
      println("接收到数据：" + count + "条")
    }

    // Start the computation  
    ssc.start()
    ssc.awaitTermination()
  }
}


