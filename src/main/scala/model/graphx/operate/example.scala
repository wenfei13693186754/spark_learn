package com.wdcloud.graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
/**
 *GraphX案例1 
 * 使用Graph object方法创建属性图对象
 */
object example {
  def main(args: Array[String]): Unit = {
    
    //spark如果工作在local模式，setMaster中的值“local[]",必须是local[*]或者local[n],n要大于1.因为在本地模式下，n代表现场数量
    //其中一个线程需要作为receiver线程用来接收数据，因为receiver要长期占用线程，其它的n-1个线程用来处理数据，所以要大于1
	  val conf = new SparkConf().setAppName("gp").setMaster("local[2]")
			  val sc = new SparkContext(conf)
	  
	  //创建一个vertices RDD,这个RDD中存放的是(3L, ("rxin", "student"))类型数据，就是vertices Tabl e中的数据
	  val users: RDD[(VertexId,(String,String))] = 
	  sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
			  (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
			  
	  //创建一个edges RDD
	  val relationships: RDD[Edge[String]] = 
	  sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
			  Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
			  
	  //定义一个默认的用户，繁殖出现有relationship但是没有user的情况
	  val defaultUser = ("John Doe","Missing")
	  
	  //创建一个原始的Graph
	  val graph = Graph(users,relationships,defaultUser)
	  println(graph+"11")
	  //从graph中解析出相应的顶点和边
	  
	  //从graph中解析出所有职位是postdoc的user，并计算数量
	  //case类似于Java中的switch case
	  val ver = graph.vertices.filter{case (id,(name,pos))=>pos=="postdoc"}.count()
	  
	  //从graph中解析出所有src是dst，并计算数量
	  val edg = graph.edges.filter { e => e.srcId > e.dstId }.count()
	  
	  //从graph中解析出三元组视图
	  val facts : RDD[String] = 
	    graph.triplets.map(triplet =>
	     triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
facts.collect.foreach(println(_))
	
    println(facts.toString())
  }
}