package model.streaming.streaming_kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import java.util.HashMap
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

/**
 * Consumes messages from one or more topics in kafka and does wordcount
 * 		Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>
 * 		<zkQquorum> is a list of one or more zookeeper servers that make quorum
 * 		<group> is the name of kafka consumer group
 * 		<topics> is a list of one or more kafka topics to consume from
 * 		<numThreads> is the number of threads the kafka consumer should use
 *
 * Example:
 *    `$ bin/run-example \
 *      org.apache.spark.examples.streaming.KafkaWordCount zoo01,zoo02,zoo03 \
 *      my-consumer-group topic1,topic2 1`
 */
object KafkaWordCount {
  def main(args: Array[String]): Unit = {
    //    if (args.length < 4) {
    //      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
    //      System.exit(1)
    //    }
		//  val Array(zkQuorum, groupId, topics, numThreads) = args
    val brokers = "192.168.6.83:9092,192.168.6.84:9092,192.168.6.89:9092"
    val topics = "T-USER-LOGINRECORD-RESULT"
    val groupId = "test-consumer-group"
    val numThreads = 1
    val conf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("hdfs://192.168.6.71:9000/graph/checkpoint")

    val topicMap = topics.split(",").map { (_, numThreads) }.toMap

    val lines = KafkaUtils.createStream(ssc, brokers, groupId, topicMap).map(_._2)
    val words = lines.flatMap { x => x.split(" ") }
    val wordCounts = words.map { x => (x, 1L) }.reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(10), 2)
    lines.print

    ssc.start()
    ssc.awaitTermination()
  }
}

//Produces some random words between 1 and 100
//object KafkaWordCountProducer {
//  def main(args: Array[String]): Unit = {
//    if (args.length < 4) {
//      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
//        "<messagesPerSec> <wordsPerMessage>")
//      System.exit(1)
//    }
//
//    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args
//
//    //zookeeper connection properties
//    val props = new HashMap[String, Object]()
//    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
//    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
//    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
//
//    val producer = new KafkaProducer[String, String](props)
//
//    //send some message 
//    while (true) {
//      (1 to messagesPerSec.toInt).foreach { messageNum =>
//        val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString()).mkString(" ")
//
//        val message = new ProducerRecord[String, String](topic, null, str)
//        producer.send(message)
//      }
//
//      Thread.sleep(1000)
//    }
//  }
//}



































