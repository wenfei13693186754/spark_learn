package com.wdcloud.graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * 收集邻居信息
 * def collectNeighborIds(edgeDirection: EdgeDirection): VertexRDD[Array[VertexId]]
      Collect the neighbor vertex ids for each vertex.
      edgeDirection
      the direction along which to collect neighboring vertices
      returns
      the set of neighboring ids for each vertex
      
   def  ollectNeighbors(edgeDirection: EdgeDirection): VertexRDD[Array[(VertexId, VD)]]
      Collect the neighbor vertex attributes for each vertex.
      edgeDirection
      the direction along which to collect neighboring vertices
      returns
      the vertex set of neighboring vertex attributes for each vertex
      Note
      This function could be highly inefficient on power-law graphs where high degree vertices may 
      force a large amount of information to be collected to a single location.
 */
object Demo7 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Demo1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    //Create an RDD for the vertices
    val users: RDD[(VertexId,(String,String))] = sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                       (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    //Create an RDD for Edges
    val relationships: RDD[Edge[String]] = sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(7L, 3L, "collab"), Edge(5L, 3L, "advisor"),
                       Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    //Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe","Missing")
    //Build the initial Graph
    val graph = Graph(users,relationships,defaultUser)
    val num = graph.edges.count()
    
    println("边的数量是："+num)
    //计算邻居相关函数，这些操作都是相当昂贵的，需要大量的重复信息作为他们的通信，因此相同的计算还是推荐用mapReduceTriplets
    val neighborIds: VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.Out)
    val neighbors: VertexRDD[Array[(VertexId,(String,String))]] = graph.collectNeighbors(EdgeDirection.Out)
    neighborIds.foreach(x => println(x._1+" 的邻居的id是： "+x._2.mkString(",")))
  }
}