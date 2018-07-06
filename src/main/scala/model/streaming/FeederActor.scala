package model.streaming

import akka.actor.ActorRef
import akka.actor.Actor
import scala.util.Random
import scala.collection.mutable.LinkedList
import scala.reflect.ClassTag
import org.apache.spark.streaming.receiver.ActorHelper
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import akka.actor.ActorSelection.toScala
import akka.actor.actorRef2Scala

case class SubscribeReceiver(receiverActor: ActorRef)
case class UnsubscribeReceiver(receiverActor: ActorRef)

/**
 * Sends the random content to every receiver subscribed with 1/2 second delay 
 */
class FeederActor extends Actor{
  val rand = new Random()
  var receivers: LinkedList[ActorRef] = new LinkedList[ActorRef]()
  
  val strings: Array[String] = Array("words", "may", "count")
  
  def makeMessage(): String = {
    val x = rand.nextInt(3)
    strings(x) + strings(2 - x)
  }
  
  /*
   * A thread to generate random message
   */
  new Thread(){
    override def run(){
      while(true){
        Thread.sleep(500)
        receivers.foreach { x => x ! makeMessage }
      }
    }
  }.start()
  
  
  def receive: Receive = {
    case SubscribeReceiver(receiverActor: ActorRef) =>
      println("received subscribe from %s".format(receiverActor.toString()))
      receivers = LinkedList(receiverActor) ++ receivers 
    case UnsubscribeReceiver(receiverActor: Actor) =>
      println("received unsubscribe from %s".format(receiverActor.toString()))
      receivers = receivers.dropWhile { x => x.equals(receiverActor) }
  }  
}

/**
 * A sample actor as receiver, is also simplest. This receiver actor
 * goes and subscribe to a typical publisher/feeder actor and receives
 * data.
 *
 * @see [[org.apache.spark.examples.streaming.FeederActor]]
 */
class SampleActorReceiver[T: ClassTag](urlOfPublisher: String) extends Actor with ActorHelper{
  /*
   * Construct an akka.actor.ActorSelection from the given path, which is parsed for wildcards 
   * (these are replaced by regular expressions internally). No attempt is made to verify the 
   * existence of any part of the supplied path, it is recommended to send a message and gather
   *  the replies in order to resolve the matching set of actors. 
   * 
   */
  lazy private val remotePublisher = context.actorSelection(urlOfPublisher)
  
  override def preStart(): Unit = remotePublisher ! (SubscribeReceiver(context.self))
  
  def receive: PartialFunction[Any, Unit] = {
    case msg => store(msg.asInstanceOf[T])
  }
  
  override def postStop(): Unit = remotePublisher ! UnsubscribeReceiver(context.self)
}

/**
 * A sample feeder actor
 *
 * Usage: FeederActor <hostname> <port>
 *   <hostname> and <port> describe the AkkaSystem that Spark Sample feeder would start on.
 */
//object FeederActor{
//  def main(args: Array[String]): Unit = {
//    if(args.length < 2){
//      System.err.println("Usage: FeederActor <hostname> <port>\n")
//      System.exit(1)
//    }
//    val Seq(host, port) = args.toSeq    
//    val conf = new SparkConf
//    var securityManager: SecurityManager = null
//    val actorSystem = AkkaUtils.createActorSystem("test", host, port.toInt, conf = conf,
//    securityManager = new SecurityManager())._1
//    val feeder = actorSystem.actorOf(Props[FeederActor], "FeederActor") 
//
//    println("Feeder started as:" + feeder)
//
//    actorSystem.awaitTermination()
//  }
//}


/**
 * A sample word count program demonstrating the use of plugging in
 * Actor as Receiver
 * Usage: ActorWordCount <hostname> <port>
 *   <hostname> and <port> describe the AkkaSystem that Spark Sample feeder is running on.
 *
 * To run this example locally, you may run Feeder Actor as
 *    `$ bin/run-example org.apache.spark.examples.streaming.FeederActor 127.0.1.1 9999`
 * and then run the example
 *    `$ bin/run-example org.apache.spark.examples.streaming.ActorWordCount 127.0.1.1 9999`
 */
object ActorWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        "Usage: ActorWordCount <hostname> <port>")
      System.exit(1)
    }

    val Seq(host, port) = args.toSeq
    val sparkConf = new SparkConf().setAppName("ActorWordCount")
    // Create the context and set the batch size
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    /*
     * Following is the use of actorStream to plug in custom actor as receiver
     *
     * An important point to note:
     * Since Actor may exist outside the spark framework, It is thus user's responsibility
     * to ensure the type safety, i.e type of data received and InputDstream
     * should be same.
     *
     * For example: Both actorStream and SampleActorReceiver are parameterized
     * to same type to ensure type safety.
     */

//    val lines = ssc.actorStream[String](  
//      Props[String](new SampleActorReceiver[String]("akka.tcp://test@%s:%s/user/FeederActor".format(
//        host, port.toInt))), "SampleReceiver")  
//
//    // compute wordcount
//    lines.flatMap(_.split("\\s+")).map(x => (x, 1)).reduceByKey(_ + _).print()
//
//    ssc.start()
//    ssc.awaitTermination() 
  }
}
// scalastyle:on println




