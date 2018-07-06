package model.streaming

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Logging
import java.net.Socket
import java.io.BufferedReader
import java.io.InputStreamReader
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

/**
 * Spark Streaming 可以从任意数据源中接收数据，超出了其内置支持的流数据源（即，超出了Flume、Kafka、Kinesis、文件、套接字等）
 * 但是这要求开发人员实现一个接收器，该接收器是自定义的，用于从相关数据源接收数据。
 *
 * 下边案例就是一个自定义的receiver,接收从socket中不断发来的流式数据
 *
 */
class CustomReceiverDemo(host: String, port: Int)
    extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  def onStart() {
    //start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    //There is nothing much to do as the thread calling receive()
    //is designed to stop by itself if isStop() returns false
  }

  /**
   * create a socket connection and receive data until receiver is stoped
   */
  private def receive() {
    var socket: Socket = null
    var userInput: String = null

    try {
      //Connect to host:port
      socket = new Socket(host, port)

      //until stoped or connection broken continue reading
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, "UTF-8"))
      userInput = reader.readLine()
      while (!isStopped && userInput != null) {
        store(userInput)
        
        userInput = reader.readLine()
      }

      reader.close()
      socket.close()

      //Restart in an attempt to connect again when server is active again
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        //restart if could not connect to server
        restart("Error connecting to " + host + ":" + port, e)

      case t: Throwable =>
        //restart if there is any other error
        restart("Error receiving data", t)
    }

  }
}

/**
 * 自定义的receiver可以被SparkStreaming使用，如下案例
 */
object CustomReceiverDemo {
  def main(args: Array[String]): Unit = {
    if(args.length < 2){
      System.err.println("Usage: CustomReceiver <hostname> <port>")
      System.exit(1)
    }
    
    val conf = new SparkConf().setAppName("customReceiver").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(2))
    
    val lines = ssc.receiverStream(new CustomReceiverDemo(args(0), args(1).toInt))
    val words = lines.flatMap(_.split(" ")) 
    val wordCounts = words.map { x => (x, 1) }.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}









