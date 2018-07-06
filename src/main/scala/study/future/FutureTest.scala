package study.future

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Future 所谓的非阻塞，实际是把其中的任务放到了新的线程中去做，从而实现了并发和当前线程非阻塞的效果。而并发部分能不能执行，要看有没有线程资源可用
 */
object FutureTest {
  def main(args: Array[String]): Unit = {
    (1 to 5).foreach { i =>
      testFuture(i)
      println(s"run testFuture $i")
    }
    println("all future started")
  }
  
  def testFuture(i: Int) = Future {
    println(s"${Thread.currentThread()}, i = $i")
    Thread sleep(3000)
    i * 2
  }
}
