package com.wdcloud.graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.impl.GraphImpl

/**
 * mask结构性操作
 */
object MaskDemo {
  def main(args: Array[String]): Unit = {
    //1.创建本地sc对象
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
    
    //Given a graph where the vertex property is the out degree
    val inputGraph: Graph[Int,String] = graph.outerJoinVertices(graph.outDegrees)((vid,_,degOps) => degOps.getOrElse(0))
    //Construct a graph where each edge contains the weight   
    //and each vertex is the initial PageRank
    val outputGraph: Graph[Double,Double] = inputGraph.mapTriplets(triplet => 1.0/triplet.srcAttr).mapVertices((id,_) => 1.0)
    
           
    //run Connected Components
    val ccGraph = graph.connectedComponents()//No longer contains missing field
    //Remove missing vertices as well as the edges to connected to them
    val validGraph = graph.subgraph(vpred = (id,attr) => attr._2 != "Missing")
    //Restrict the answer to the valid subgraph
    val validCCGraph = ccGraph.mask(validGraph)
    
    val v1 = graph.vertices.filter(x => x._1 == 0.0)
    graph.subgraph(
        epred = e=>e.srcId < e.dstId,
        vpred = (id, attr) => attr._2 != "Missing")
  }
}