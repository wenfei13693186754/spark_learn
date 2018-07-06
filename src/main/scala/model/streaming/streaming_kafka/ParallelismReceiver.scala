package model.streaming.streaming_kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder

/**
 * Spark Streaming同时从多个数据流中读取数据。
 * 接收数据需要数据是被反序列化后的并且被保存在spark上的。
 * 如果数据接收成为了性能瓶颈，那么可以考虑提高数据接收的并行度。
 * 提高并行度的一个办法就是通过创建多个输入流，不同的输入流读取数据源的不同的分区来实现。
 * 例如，kafka读取两个topic的数据流，那么可以使用两个Input DStream来分别读取不同topic中的数据。
 * 这样可以并行执行，可以提高数据的吞吐量。然后使用union等函数将两个DStream进行合并就可以得到一个
 * 单独的Input DStream了。
 *
 */
object ParallelismReceiver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("parallelismReceiver").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(2))

    val brokers = "192.168.6.83:9092,192.168.6.84:9092,192.168.6.89:9092"
    val numThreads = 1
    val topics = "TEST_LOGINLOG".split(",").map { x => (x, numThreads) }.toMap
    val groupId = "my-consumer-group"

    val numStreams = topics.size
    //这里的这个操作会将topic中的内容分给5个worker去读取
    val kafkaStreams = (1 to numStreams).map {i => KafkaUtils.createStream(ssc, brokers, groupId, topics)}
    
    val unifiedStream = ssc.union(kafkaStreams) 
    unifiedStream.print()
  }  
}

