package model.streaming.streaming_kafka_zookeeper

import java.io.File

import org.I0Itec.zkclient.ZkClient
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import akka.event.slf4j.Logger
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import  org.apache.spark.streaming.kafka.OffsetRange
object WriteOffsetToZk extends Serializable {
  val logger = Logger(this.getClass.getName)
  def createContext(brokers: String, topics: String, outputPath: String, checkpointDirectory: String): StreamingContext = {
    //if you do not see this printed, that means the StreamingContext has been loaded from the new checkpoint
    println("Creating new Context")
    val outputFile = new File(outputPath)
    if (outputFile.exists()) {
      outputFile.delete()
    }

    val conf = new SparkConf().setAppName("RecoverableNetworkWordCount").setMaster("spark://192.168.6.83:6066").set("deploy-mode", "cluster")
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint(checkpointDirectory)

    val topicSet = topics.split(",").toSet
    val kafkaParam = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")

    //create kafka messages
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topicSet)
    lines.print()
    //1.Hold a reference to the current offset ranges, so it can be used downstream
    var offsetRanges = Array[OffsetRange]()
    lines.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD { msg =>
      writeOffsetToZk(topics, offsetRanges, msg) 
    }
    ssc
  }

  //将offset写入到zk上
  def writeOffsetToZk(topics: String,offsetRanges: Array[OffsetRange], msg: RDD[(String, String)]) {
    //write offset to zk
    //2.Create a topicDirs
    val topicDirs = new ZKGroupTopicDirs("test_spark_streaming_group", topics)
    //3.获取zk中的路径，这里会变成 /consumers/test_spark_streaming_group/offsets/topic_name
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"
    //4.创建一个zk客户端
    val zkClient = new ZkClient("192.168.6.83:2181")
    for (o <- offsetRanges) {
      val zkPath = s"${zkTopicPath}/${o.partition}"
      ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString())
      logger.warn(s"Offset update: set offset of ${o.topic}/${o.partition} as ${o.untilOffset.toString}")
      println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
    }
    msg.foreachPartition(
      message => {
        while (message.hasNext) { 
          //logger.warn(s"@^_^@   [" + message.next() + "] @^_^@")
        }
      })
  }

  def main(args: Array[String]): Unit = {
    //create kafka param
    val brokers = "192.168.6.83:9092,192.168.6.84:9092,192.168.6.89:9092"
    val topics = "TEST_LOGINLOG"
    val checkpointDirectory = "hdfs://192.168.6.71:9000/streaming/checkpoint"
    val outputPath = "checkpointData"
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => createContext(brokers, topics, outputPath, checkpointDirectory))
    ssc.start()
    ssc.awaitTermination()
  }
}

/**
 * Use this singleton to get or register a Broadcast variable
 */
object WordBlacklist {

  @volatile private var instance: Broadcast[Seq[String]] = null

  def getInstance(sc: SparkContext): Broadcast[Seq[String]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val wordBlacklist = Seq("a", "b", "c")
          instance = sc.broadcast(wordBlacklist)
        }
      }
    }
    instance
  }
}

/**
 * Use the singleton to get or register an Accumulator
 */
object DroppedWordsCounter {
  @volatile private var instance: Accumulator[Int] = null

  def getInstance(sc: SparkContext): Accumulator[Int] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.accumulator(0, "WordsInBlacklistCounter")
        }
      }
    }
    instance
  }
}





