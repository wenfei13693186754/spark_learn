package model.streaming

import java.io.File
import java.nio.charset.Charset
import org.apache.spark.Accumulator
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.kafka.KafkaUtils
import com.google.common.io.Files
import akka.event.slf4j.Logger
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

/**
 * Counts words in text encoded with UTF8 received from the network every second. This example also
 * shows how to use lazily instantiated singleton instances for Accumulator and Broadcast so that
 * they can be registered on driver failures.
 *
 * Usage: RecoverableNetworkWordCount <hostname> <port> <checkpoint-directory> <output-file>
 *   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive
 *   data. <checkpoint-directory> directory to HDFS-compatible file system which checkpoint data
 *   <output-file> file to which the word counts will be appended
 *
 * <checkpoint-directory> and <output-file> must be absolute paths
 *
 * To run this on your local machine, you need to first run a Netcat server
 *
 *      `$ nc -lk 9999`
 *
 * and run the example as
 *
 *      `$ ./bin/run-example org.apache.spark.examples.streaming.RecoverableNetworkWordCount \
 *              localhost 9999 ~/checkpoint/ ~/out`
 *
 * If the directory ~/checkpoint/ does not exist (e.g. running for the first time), it will create
 * a new StreamingContext (will print "Creating new context" to the console). Otherwise, if
 * checkpoint data exists in ~/checkpoint/, then it will create StreamingContext from
 * the checkpoint data.
 *
 * Refer to the online documentation for more details.
 */
object RecoverableNetworkWordCount {
  val logger = Logger(this.getClass.getName)
  def createContext(brokers: String, topics: String, outputPath: String, checkpointDirectory: String): StreamingContext = {
    val t0 = System.currentTimeMillis()
    //if you do not see this printed, that means the StreamingContext has been loaded from the new checkpoint
    println("Creating new Context")
    val outputFile = new File(outputPath)
    if (outputFile.exists()) {
      outputFile.delete()
    }
    val conf = new SparkConf().setAppName("RecoverableNetworkWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint(checkpointDirectory)
    val sc = ssc.sparkContext

    val topicSet = topics.split(",").toSet
    val kafkaParam = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")

    //create kafka messages
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topicSet).map(_._2)
    lines.print()
    val words = lines.flatMap { x => x.split(" ") }
    val wordCounts = words.map { x => (x, 1) }.reduceByKey(_ + _)
    //开始对每个分区中的rdd进行操作
    wordCounts.foreachRDD { (rdd: RDD[(String, Int)], time: Time) =>
      //Get or register the blacklist Broadcast
      val blacklist = WordBlacklist.getInstance(rdd.sparkContext)
      //Get or register an Accumulator
      val droppedWordsCounter = DroppedWordsCounter.getInstance(rdd.sparkContext)
      val counts = rdd.filter {
        case (word, count) =>
          droppedWordsCounter.add(count)
          if (blacklist.value.contains(word)) {
            false
          } else {
            true
          }
      }.collect.mkString(",")
      val output = "Count at time " + time + " " + counts
      //println(output)
      println("Dropped " + droppedWordsCounter.value + " word(s) totally")
      println("Appending to " + outputFile.getAbsolutePath)
      Files.append(output + "\n", outputFile, Charset.defaultCharset())
    }
    ssc
  }

  def main(args: Array[String]): Unit = {
    //    if (args.length != 4) {
    //      System.err.println("Your arguments were " + args.mkString("[", ", ", "]"))
    //      System.err.println(
    //        """
    //          |Usage: RecoverableNetworkWordCount <hostname> <port> <checkpoint-directory>
    //          |     <output-file>. <hostname> and <port> describe the TCP server that Spark
    //          |     Streaming would connect to receive data. <checkpoint-directory> directory to
    //          |     HDFS-compatible file system which checkpoint data <output-file> file to which the
    //          |     word counts will be appended
    //          |
    //          |In local mode, <master> should be 'local[n]' with n > 1
    //          |Both <checkpoint-directory> and <output-file> must be absolute paths
    //        """.stripMargin
    //      )
    //      System.exit(1)
    //    }  

    //create kafka param
    val brokers = "192.168.6.83:9092,192.168.6.84:9092,192.168.6.89:9092"
    val topics = "TEST_LOGINLOG"
    //val checkpointDirectory = "hdfs://192.168.6.71:9000/streaming/checkpoint"
    val checkpointDirectory = "D:\\workplace\\java\\wdcloud\\person_statistics\\checkpoint"
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





