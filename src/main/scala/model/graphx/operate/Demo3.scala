package com.wdcloud.graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
/**
 * 连接操作
 * 	outerJoinVertices
 * 	joinVertices
 * 
 */
object Demo3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Demo1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    //Create an RDD for the vertices
    val users: RDD[(VertexId,(String,String))] = sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                       (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    //Create an RDD for Edges
    val relationships: RDD[Edge[String]] = sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                       Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    //Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe","Missing")
    //Build the initial Graph
    val graph = Graph(users,relationships,defaultUser)
    graph.triplets.foreach(x =>println("旧："+x.srcAttr))
    val outDeg: RDD[(VertexId,Int)] = graph.outDegrees
    val newGraph = graph.outerJoinVertices(outDeg)(
      (vid,data,optDeg) => optDeg.getOrElse(0)    
    )
    graph.triplets.foreach(x =>println("__旧："+x.srcAttr))
    newGraph.triplets.foreach(x =>println("源顶点  "+x.srcId+"的属性是："+x.srcAttr+"||目的顶点  "+x.dstId+"的属性是："+x.dstAttr))
    
  }
}