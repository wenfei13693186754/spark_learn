package study.thread

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.concurrent.Executors
import java.util.concurrent.Callable
import java.util.ArrayList
import java.util.concurrent.Future
import collection.JavaConverters._
import akka.event.slf4j.Logger

object mutilThread {
  private val logger = Logger(this.getClass.getName)
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("multi task submit ")
      .setMaster("local[*]")
    //实例化spark context
    val sc = new SparkContext(sparkConf) 
    //保存任务返回值
    val list = new ArrayList[Future[String]]()
    //并行任务读取的path  
    val task_paths = new ArrayList[String]()
    task_paths.add("E:\\实时在线系统\\实时用户统计\\测试\\test_data-2.txt")
    task_paths.add("E:\\实时在线系统\\实时用户统计\\测试\\test_data-1.txt")
    task_paths.add("E:\\实时在线系统\\实时用户统计\\测试\\test_data.txt")

    //线程数等于path的数量
    val nums_threads = task_paths.size()
    //构建线程池
    val executors = Executors.newFixedThreadPool(nums_threads)
    for (i <- 0 until nums_threads) {
      val task = executors.submit(new Callable[String] {
        override def call(): String = {
          val count = sc.textFile(task_paths.get(i)).count() //获取统计文件数量
          "当前线程："+Thread.currentThread().getName+"   "+task_paths.get(i) + " 文件数量： " + count
        }
      })
      list.add(task) //添加集合里面
    }
    
    //遍历获取结果
    list.asScala.foreach(result => {
      println(result.get)
    })
    //停止spark
    sc.stop()

  }
}