package spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.StreamingContext
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.Seconds
import org.apache.spark.TaskContext
import com.google.common.hash.Hashing

/**
 * Semantics of output operations
 *
 * Output operations (like foreachRDD) have at-least once semantics, that is, the transformed data may get written to an external entity more than once in the event of a worker failure. While this is acceptable for saving to file systems using the saveAs***Files operations (as the file will simply get overwritten with the same data), additional effort may be necessary to achieve exactly-once semantics. There are two approaches.
 *
 * Idempotent updates: Multiple attempts always write the same data. For example, saveAs***Files always writes the same data to the generated files.
 *
 * Transactional updates: All updates are made transactionally so that updates are made exactly once atomically. One way to do this would be the following.
 * Use the batch time (available in foreachRDD) and the partition index of the RDD to create an identifier. This identifier uniquely identifies a blob data in the streaming application.
 *
 * Update external system with this blob transactionally (that is, exactly once, atomically) using the identifier. That is, if the identifier is not already committed, commit the partition data and the identifier atomically. Else, if this was already committed, skip the update.
 *
 * dstream.foreachRDD { (rdd, time) =>
 * rdd.foreachPartition { partitionIterator =>
 * val partitionId = TaskContext.get.partitionId()
 * val uniqueId = generateUniqueId(time.milliseconds, partitionId)
 * // use this uniqueId to transactionally commit the data in partitionIterator
 * }
 * }
 *
 *
 */
object SemanticsOfOutputOperations {
  def main(args: Array[String]): Unit = {

    //create kafka param
    val brokers = "192.168.6.83:9092,192.168.6.84:9092,192.168.6.89:9092"
    val topics = "TEST_LOGINLOG"
    val checkpointDirectory = "D:\\workplace\\java\\wdcloud\\person_statistics\\checkpoint"

    val conf = new SparkConf().setAppName("RecoverableNetworkWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint(checkpointDirectory)
    val sc = ssc.sparkContext

    val topicSet = topics.split(",").toSet
    val kafkaParam = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")

    //create kafka messages
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topicSet).map(_._2)
    lines.foreachRDD { (rdd, time) =>
      rdd.foreachPartition { partitionIterator =>
        val partitionId = TaskContext.get().partitionId()
        val uniqueId = generateUniqueId(time.milliseconds, partitionId)
        //如果外部存储系统没有uniqueId,那么将partitionIterator中内容输出，并将uniqueId也输出，
        
        //如果外部存储系统已经有了uniqueId,那么partitionIterator中内容被跳过
      }
    }
  }

  def generateUniqueId(time: Long, partitionId: Int): Long = {
    Hashing.md5().hashString(time + "" + partitionId).asLong()
  }

  def hashId(name: String, str: String) = {
    Hashing.md5().hashString(name + "" + str).asLong()
  }
}












