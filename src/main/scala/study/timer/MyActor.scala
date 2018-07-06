package study.timer

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.util.Timeout
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * scala定时调度机制
 */
class MyActor extends Actor {

  def receive = {
    case para: String => println(para)
    case _            => ()
  }
}

object DemoTest {
  val system = ActorSystem("mySystem")
  val act1 = system.actorOf(Props[MyActor], "first")
  def main(args: Array[String]): Unit = {
    //timerSendMsg
    timerFun
  }

  //定时发送消息给receiver
  def timerSendMsg() {
    val cancellable = system.scheduler.schedule(Duration(1000, TimeUnit.MILLISECONDS), Duration(500, TimeUnit.MILLISECONDS), act1, System.currentTimeMillis().toString())
    //cancellable.cancel()
  }

  //定时执行某个function
  def timerFun() {
    val scan = system.scheduler.schedule(Duration(1000, TimeUnit.MILLISECONDS), Duration(500, TimeUnit.MILLISECONDS))(timerSendMsg())
//    if(!scan.isCancelled)
//    scan.cancel()
  }
}



