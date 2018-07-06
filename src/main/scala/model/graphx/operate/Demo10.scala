package com.wdcloud.graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
/**
 * 过滤操作
 */
object Demo10 {
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
    graph.vertices.foreach(x => println("!!!!!"+x._2._1+" 的id是： "+x._1+" 他的职业是："+x._2._2))
    //remove the vertices in a graph with 0 outdegree
    val newGraph = graph.filter(
      graph => {
        val degrees: VertexRDD[Int] = graph.outDegrees
        graph.outerJoinVertices(degrees) {(vid, data, deg) => deg.getOrElse(0)}
      },
      vpred = (vid: VertexId, deg:Int) => deg > 0
    )
  
  newGraph.vertices.foreach(x => println(x._2._1+" 的id是： "+x._1+" 他的职业是："+x._2._2))
  
  }
}