package com.wdcloud.graphx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * Pregel API
 * 在GraphX中，更高级的Pregel操作是一个约束到图拓扑的批量同步(bulk-synchronous)并行消息抽象
 * Pregel操作者执行一系列的超级步骤（super steps），在这些步骤中，顶点从之前的超级步骤中接收进
 * 入(inbound)消息的总和，为顶点属性计算一个新的值。然后在以后的超级步骤中发送消息到邻居顶点。
 * 
 * 消息作为一个边三元组的函数被并行计算，消息计算既访问了源顶点的特征也访问了目的顶点的特征。
 * 在超级步中，没有收到消息的顶点被跳过。当没有消息遗留时，Pregel操作停止迭代，并返回最终的图。
 * 
 * 其第二个参数中的三个方法：
 * 		1.vprog: 作用在各个顶点上，用来接收来自上一步的消息，并计算出新的attr；
 * 		2.sendMsg: 接收本次计算迭代完成的消息并作用在该顶点上的out-degree上;
 * 		3. mergeMsg: 用来合并消息
 * 
 * 这里使用Pregel操作表达计算单源最短路径(singel source shortest path)
 */
object Demo8 {
	val conf = new SparkConf().setAppName("pregel").setMaster("local[*]")
	val sc = new SparkContext(conf)

	def main(args: Array[String]): Unit = {
    //demo
    //shortestPath
	  demo1()
  }
  
	def demo1(){
	  //Create an RDD for the vertices
    val users: RDD[(VertexId,(String))] = sc.parallelize(Array((1L, ("P1")), (2L, ("P2")), (3L, ("P3"))))
    //Create an RDD for Edges
    val relationships: RDD[Edge[String]] = sc.parallelize(Array(Edge(1L, 2L, "F"),Edge(1L, 3L, "F")))
    //Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe")
    //Build the initial Graph
    val graph = Graph(users,relationships,defaultUser)
    
    val pregel = graph.pregel("", 1, EdgeDirection.Out)(  
      (id, dist, newDist) => // Vertex Program
        dist+" "+newDist, 
      triplet => { // Send Message:
        Iterator((triplet.srcId, triplet.dstAttr))  
      },  
      (a,b) => a+" "+b // Merge Message：
    ) 
    
    pregel.vertices.foreach(x => println(x._1+" "+(x._2.split(" ")).mkString(",")))
	}
	
	/*
	 * 使用pregel计算最短路径
	 */
  def shortestPath(){
    val graph = GraphLoader.edgeListFile(sc, "E:\\spark\\Spark-GraphX\\data\\otherData\\web-Google\\web-Google.txt")
    //设定源顶点
    val sourceId: VertexId = 0
    
    //对图进行初始化
    /*
     *下边这段代码的意思是对所有非源顶点，将顶点的属性值设置为无穷，因为我们打算将所有顶点的属性值用于保存源点到该点之间的最短路径。
     * 在正式开始计算之前将源顶点到自己的路径长度设置为0，到其它顶点的距离设置为无穷大，如果计算过程中遇到更短的路径替换当前的长度
     * 就可以了，如果源点到该点不可达，那么路径长度自然无穷大了。 
     */
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
    
    //接下来计算最短路径
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(  
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program:  
      triplet => { // Send Message.
        println("triplet.dstAttr: "+triplet.dstAttr)
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {  
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))  
        } else {  
          Iterator.empty  
        }  
      },  
      (a,b) => math.min(a,b) // Merge Message  
    )  
   
    //打印下sssp
    val r1 = sssp.vertices.take(10).mkString("\n")
    println(r1)
//    (651447,Infinity)
//    (182316,Infinity)
//    (846729,11.0)
//    (627804,15.0)
//    (831957,10.0)
//    (512760,12.0)
//    (307248,Infinity)
//    (449586,12.0)
//    (857454,9.0)可以看出0到857454的最短路径是9，0到651447的最短路径是无穷大
 
  
  }  
  
  def  demo(){
    val vertices: RDD[(VertexId, (Int, Int))] = sc.parallelize(Array((1L, (7,-1)), (2L, (3,-1)),(3L, (2,-1)), (4L, (6,-1))))
    val relationships: RDD[Edge[Boolean]] = sc.parallelize(Array(Edge(1L, 2L, true), Edge(1L, 4L, true),
                       Edge(2L, 4L, true), Edge(3L, 1L, true), 
                       Edge(3L, 4L, true)))
                       
    val graph = Graph(vertices,relationships)
    //graph.vertices.collect().foreach(println)
    
    val minGraph = graph.pregel(initialMsg, 
                            Int.MaxValue, 
                            EdgeDirection.Out)(
                            vprog,
                            sendMsg,
                            mergeMsg)
    minGraph.vertices.collect.foreach{
      case (vertexId, (value, original_value)) => println(value)
    }
    
  }
  val initialMsg = 9999
  //作用在顶点上，用来接收上一步发来的消息，并计算出新的attr
  //vertexId: 顶点id   value: 顶点数据类型    message:The Pregel message type
  def vprog(vertexId: VertexId, value: (Int, Int),message: Int): (Int, Int) = {
    if (message == initialMsg) {
      println(value)
      value
    }else{
      println("message+else "+message)
      (message min value._1, value._1)
    }
  }
  
  //接收本次计算迭代完成的消息并作用在该顶点的out edges上（sendMsg）
  def sendMsg(triplet: EdgeTriplet[(Int, Int), Boolean]): Iterator[(VertexId, Int)] = {
    val sourceVertex = triplet.srcAttr
    println("sourceVertex: "+sourceVertex)
    if (sourceVertex._1 == sourceVertex._2)
      Iterator.empty
    else 
      Iterator((triplet.dstId, sourceVertex._1))
  }
  
  def mergeMsg(msg1: Int, msg2: Int): Int = msg1 min msg2
  
  
}



