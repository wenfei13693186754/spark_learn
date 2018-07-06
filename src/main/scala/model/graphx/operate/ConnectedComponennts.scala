package com.wdcloud.graphx

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 联通体算法
 */
object ConnectedComponennts {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ConnectedComponennts").setMaster("local[*]")
    val sc = new SparkContext(conf)
	  // Load the graph as in the PageRank example
	  val graph = GraphLoader.edgeListFile(sc, "E:\\followers.txt")
    // 用上边的图框架调用connectedComponents算法，
	  val cc = graph.connectedComponents().vertices
	  
	  //将上边的cc(顶点属性)和用户进行关系连接
    //首先读取一个包含了用户信息的文件，然后调用map函数，将文件中每一行数据进行split操作
	  //并返回存储了集合中指定数据的RDD
	  val users = sc.textFile("E:\\users.txt").map { line =>
  	  val fields = line.split(",")
  	  (fields(0).toLong, fields(1))
	  }
    
    //这里具体实现了将cc和用户列表一一对应起来
    //从map函数的内容可以看出来按id来进行连接，但是返回的结果只包含用户名和它对应的cc
	  val ccByUsername = users.join(cc).map {
	    case (id, (username, cc)) => (username, cc)
	  }
    // Print the result
	  println(ccByUsername.collect().mkString("\n"))
    
  }
}