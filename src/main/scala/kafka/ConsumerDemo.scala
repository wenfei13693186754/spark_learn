package kafka

import java.util.Properties
import kafka.consumer.ConsumerConfig
import kafka.consumer.Consumer

/**
 * kafka消费者
 *
 */
object ConsumerDemo {
  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put("zookeeper.connect", "192.168.6.83:2181,192.168.6.84:2181,192.168.6.89:2181")
    props.put("group.id", "xyf")
    props.put("client.id", "test")
    props.put("consumer.id", "TEST_LOGINLOG")
    props.put("auto.offset.reset", "largest")
    props.put("auto.commit.enable", "true")
    props.put("auto.commit.interval.ms", "100")

    val consumerConfig = new ConsumerConfig(props)
    val consumer = Consumer.create(consumerConfig)

    val topicCountMap = Map("TEST_LOGINLOG" -> 1)
    val consumerMap = consumer.createMessageStreams(topicCountMap)
    val streams = consumerMap.get("TEST_LOGINLOG").get
    for (stream <- streams) {
      val it = stream.iterator()
      while (it.hasNext()) {
        val messageAndMetadata = it.next()
        val message = s"Topic:${messageAndMetadata.topic}, GroupID:xyf, Consumer ID:TEST_LOGINLOG, PartitionID:${messageAndMetadata.partition}, " +
          s"Offset:${messageAndMetadata.offset}, Message Key:${new String(messageAndMetadata.key())}, Message Payload: ${new String(messageAndMetadata.message())}"

        System.out.println(message);
      }
    }
  }
}