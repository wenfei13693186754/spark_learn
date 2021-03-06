package kafka

import org.apache.spark._
import kafka.producer.ProducerConfig
import kafka.producer.Producer
import java.util.Properties
import kafka.producer.KeyedMessage
import kafka.serializer.StringEncoder
import akka.event.slf4j.Logger
import org.apache.spark.rdd.RDD
import kafka.serializer.StringEncoder
import com.alibaba.fastjson.JSONObject

/**
 * 定义kafka消息生产者，接收推荐结果，并将推荐结果放到kafka中
 *
 * kafka初始化：
 * 	METADATA_BROKER_LIST=192.168.6.89:9092,192.168.6.83:9092,192.168.6.84:9092
 * SERIALIZER_CLASS=kafka.serializer.StringEncoder
 * REQUEST_REQUIRED_ACKS=-1
 *
 * TOPIC=TEST_LOGINLOG
 * ZOOKEEPER_CONNECT=192.168.6.89:2181,192.168.6.83:2181,192.168.6.84:2181
 * GROUP_ID=test-consumer-group
 * ZOOKEEPER_SESSION_TIMEOUT_MS=1000
 * ZOOKEEPER_SYNC_TIME_MS=200
 * AUTO_COMMIT_INTERVAL_MS=1000
 * AUTO_OFFSET_RESET=smallest
 */
object Producer extends Serializable {

  val logger = Logger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kafka").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val topic = "TEST_LOGINLOG"
    var data = 0
    val key = "test"
    while (true) {
      sends(topic, key, data + "#")
      data = data + 1
      Thread.sleep(500)
      println("发送出去的数据量是："+data)
    }

  }

  def sends(topic: String, key: String, data: String) {
    val producer = new Producer[String, String](new ProducerConfig(getProducerConfig()))
    producer.send(new KeyedMessage[String, String](topic, key, data))
    producer.close()
  }

  def getProducerConfig(): Properties = {
    val props = new Properties()
    //配置kafka端口
    props.setProperty("metadata.broker.list", "192.168.6.89:9092,192.168.6.83:9092,192.168.6.84:9092")
    //配置value的序列化类
    props.setProperty("serializer.class", classOf[StringEncoder].getName)
    //配置key的序列化类
    props.setProperty("key.serializer.class", classOf[StringEncoder].getName)
    props.setProperty("request.required.acks", "1")
    //props.setProperty("TOPIC","TEST_LOGINLOG")
    props
  }

  //将推荐结果放到kafka中
  def sendMsgToKafka(jsonResult: RDD[(String, String, JSONObject)], namespace: String) {
    jsonResult.foreach { x =>
      sends(namespace + ".T_REC_RESULT", x._2, x._3.toString())
    }
    logger.warn("将消息放到kafka中成功")
  }
}  









