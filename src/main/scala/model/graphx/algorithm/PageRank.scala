package com.wdcloud.graphx

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}

/**
 * pagerank算法，可以实现网页排名类似的操作
 */
object PageRank {
  def main(args: Array[String]): Unit = {
	  val conf = new SparkConf().setAppName("PageRank").setMaster("local[2]")
	  val sc = new SparkContext(conf)
	  //edgeListFile读取边列表文件的时候，要求文件内容必须是vid vid 
	  val graph = GraphLoader.edgeListFile(sc,"E:\\spark\\Spark-GraphX\\data\\followers.txt")
	  //用上边的图框架调用pagerank（动态）算法，静态调用的方法名是staticPageRank(Int)
	  //传入的这个参数的值越小PageRank计算的值就越精确，如果数据量特别大而传入的参数值又特别小
	  //的情况下就会导致巨大的计算任务和计算时间。 
	  //vertices将返回顶点属性
	  val ranks = graph.pageRank(0.001).vertices
	  ranks.foreach(x =>println(x._1+"****"+x._2))
	  //将上边得到的ranks（顶点属性）和用户进行关系连接
	  // 首先也是读取一个包含了用户信息的文件，然后调用了一个map函数，即将文件里的每行数据
	  //按 ”,” 切开并返回存储了处理后数据的RDD  
	  val users = sc.textFile("E:\\spark\\Spark-GraphX\\data\\users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
	  
	  //这里具体实现了将ranks和用户列表一一对应起来
	  //从map函数的内容可以看出来按id来进行连接，但是返回的结果只包含用户名和它相应的rank
	  val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    
	  //打印結果
	  /*
	   * (xiaoming,0.15)
       (Hanmeimei,0.6644122940740325)
       (ladygaga,1.33561785364765)
       (BarackObama,1.4047282141296686)
       (John,0.9379075857841748)
       (Polly,1.210381868409488)
	   */
	  println(ranksByUsername.collect().mkString("\n"))
	  val PR = Array.fill(1)(1.0)
  }
}