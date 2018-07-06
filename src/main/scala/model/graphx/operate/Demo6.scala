package com.wdcloud.graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * 计算顶点的出度、入度和总度
 */
object Demo6 {
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
    //Notice that there is a user 0(for which we have no information) connected to user
    //4(peter) and 5(franklin)
    graph.triplets.map(triplet => triplet.srcAttr._1+" is the "+triplet.attr+" of "+triplet.dstAttr._1).collect().foreach { println(_) }
    
    //Compute the max degree
    val maxInDegree:(VertexId,Int) = graph.inDegrees.reduce(max)
    
    val maxOutDegree:(VertexId,Int) = graph.outDegrees.reduce(max)
    val maxDegree:(VertexId,Int) = graph.degrees.reduce(max)
    println("最大的入度是："+maxInDegree+"|||"+"最大的出度是："+maxOutDegree+"|||"+"最大的度是："+maxDegree)
  }
  //Define a reduce operation to compute the highest degree vertex
  //(VerttexId,Int):顶点id,顶点的度
  def max(a:(VertexId,Int),b:(VertexId,Int)): (VertexId,Int) = {
		  if (a._2>b._2) a else b
  }
}