package model.streaming.streaming_kafka

import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.StreamingContext
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.HasOffsetRanges

/**
 * 下边案例可以获取到每次消耗kafka中信息，kafka的指针偏移量
 */
object AccessOffsets {
  def main(args: Array[String]): Unit = {
    //hold a reference to the current offset ranges, so it can be used downstream
    var offsetRanges = Array[OffsetRange]()

    val brokers = "192.168.6.83:9092,192.168.6.84:9092,192.168.6.89:9092"
    val topics = "TEST_LOGINLOG"
    // Create context with 2 second batch interval  
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    directKafkaStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.map { rdd =>
      rdd
    }.foreachRDD { rdd =>
      for (o <- offsetRanges) {
        println(s"主题名：${o.topic}  分区：${o.partition}  游标开始：${o.fromOffset}  游标结束：${o.untilOffset}")
      }
    }
    
    ssc.start()
    ssc.awaitTermination()
  }
}