package kafka

import java.util.HashMap
import java.util.Properties
import java.util.Random

import scala.io.Source

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.alibaba.fastjson.JSONObject

import akka.event.slf4j.Logger
import scala.reflect.ClassTag
import scala.util.Try
import org.apache.kafka.clients.producer.RecordMetadata
import scala.util.{ Failure, Success, Try }
import org.apache.kafka.clients.producer.Callback

object ProducerRec extends Serializable {

  val logger = Logger(this.getClass.getName)
  val brokers = "192.168.6.83:9092,192.168.6.84:9092,192.168.6.89:9092"
  val topics = "T-USER-LOGINRECORD"
  //val topics = "TEST_LOGINLOG"
  val props = new HashMap[String, Object]()
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.ACKS_CONFIG, "-1")
  props.put(ProducerConfig.RETRIES_CONFIG, "5")
  props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "500")
  props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "120000")
  val producer = new KafkaProducer[String, String](props)

  def main(args: Array[String]): Unit = {
    //sendHbaseDataToKafka()
    //测试重复id数据
    sendTestData()
  }
  
  def sendHbaseDataToKafka() {
    var num = 0
    val arr = readDataFromHbase.take(50)
    while (num < 3) {

      arr.foreach { x =>
        //RDD(PK, LOGINID, USERTYPE, LOGINEND, ADDRESS, USERUA, STATUS)
        val json = new JSONObject
        json.put("PK", x._1)
        json.put("INFO.LOGINID", x._2)
        json.put("INFO.USERTYPE", x._3)
        json.put("INFO.LOGINEND", x._4)
        json.put("INFO.IP", x._5)
        json.put("INFO.USERAGENT", x._6)
        json.put("INFO.STATUS", x._7)
        //logger.warn("发送数据："+json.toString())
        sendMsgToKafka(json.toString())

        Thread.sleep(5)
      }
      num = num + 1
    }
  }

  /*
   * 造测试数据
   * 每10s发送一条
   */
  def sendTestData() {
    val data = Source.fromFile("E:\\实时在线系统\\实时用户统计\\测试\\test_data-2.txt").getLines()
    var num = 0
    while (data.hasNext) {
      val arr = data.next().split("#")
      arr.update(3, System.currentTimeMillis() - 30000 + "")
      val json = new JSONObject
      json.put("PK", arr(0))
      json.put("INFO.LOGINID", arr(1))
      json.put("INFO.USERTYPE", arr(2))
      json.put("INFO.LOGINEND", arr(3))
      json.put("INFO.IP", arr(4))
      json.put("INFO.USERAGENT", arr(5))
      json.put("INFO.STATUS", arr(6))

      sendMsgToKafka(json.toString())
      num = num + 1
      //println(num + "*****" + json.toString())
      //Thread.sleep(100)
    }
  }

  def sendMsgToKafka(str: String) {
    val message = new ProducerRecord[String, String](topics, null, str)
    producer.send(message, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        val result =
          if (exception == null) println("数据发送成功")
          else println("发送失败")
      }
    })
  }

  def sendWithCallback(record: ProducerRecord[String, String])(callback: Try[RecordMetadata] => Unit) {
    val info = producer.send(record, producerCallBack(callback))
    println("发送成功与否：" + info)
  }

  def producerCallBack(callback: Try[RecordMetadata] => Unit): Callback =
    new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        val result =
          if (exception == null) Success(metadata)
          else Failure(exception)
        callback(result)
      }
    }

  /**
   * 从hbase用户表中读取信息，用来实时写入到kafka供消费者消费
   */
  def readDataFromHbase(): RDD[(String, String, String, Long, String, String, String)] = {
    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", "zk1.cs,zk2.cs,zk3.cs")
    //hconf.set("hbase.zookeeper.quorum", "hrs1.dev.wdcloud.cc,hrs2.dev.wdcloud.cc,hrs3.dev.wdcloud.cc")
    hconf.set("hbase.zookeeper.property", "2181")
    hconf.set(TableInputFormat.SCAN_CACHEBLOCKS, "false")
    hconf.set(TableInputFormat.SCAN_BATCHSIZE, "10000")
    hconf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "INFO")
    hconf.set(TableInputFormat.INPUT_TABLE, "PTYHZX:T_SYS_LOGINRECORD")

    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val hbaseRDD = sc.newAPIHadoopRDD(hconf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    val count = hbaseRDD.count()
    val ran = new Random
    val userInfo = hbaseRDD.map {
      case (_, result) =>
        val rowkey = Bytes.toString(result.getRow)
        val loginId = Bytes.toString(result.getValue("INFO".getBytes, "LOGINID".getBytes))
        val userType = Bytes.toString(result.getValue("INFO".getBytes, "USERTYPE".getBytes))
        val loginTime = System.currentTimeMillis()
        val userIp = Bytes.toString(result.getValue("INFO".getBytes, "IP".getBytes))
        val userAgent = Bytes.toString(result.getValue("INFO".getBytes, "USERAGENT".getBytes))
        //(ROWKEY, LOGINID, USERTYPE, LOGINEND, ADDRESS, USERUA, STATUS)
        (rowkey, loginId, userType, loginTime, userIp, userAgent, "1")
    }
    userInfo
  }
}


