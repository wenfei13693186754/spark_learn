package com.wdcloud.graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


/**
 * 并行边的操作
 */
object Demo4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Demo1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    //Create an RDD for the vertices
    val users: RDD[(VertexId,(String,String))] = sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                       (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    //Create an RDD for Edges
    val relationships: RDD[Edge[String]] = sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                       Edge(2L, 5L, "colleague"),Edge(2L, 5L, "friends"),Edge(2L, 5L, "black"), Edge(5L, 7L, "pi")))
    //Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe","Missing")
    //Build the initial Graph
    val graph = Graph(users,relationships,defaultUser)
    
    //遍历获取用户关系
    graph.triplets.map(triplet => triplet.srcAttr._1+" is the "+triplet.attr+" of "+triplet.dstAttr._1).collect().foreach { println(_) }
    
    println("********************************************")
    
    //对graph进行重新分区
    val partGraph = graph.partitionBy(PartitionStrategy.EdgePartition1D,4)

    // 使用groupEdges语句将edge中相同Id的数据进行合并  
    val edgeGroupedGraph:Graph[(String, String), (String)] = partGraph.groupEdges(merge = (e1, e2) => (e1 +"||"+ e2))  
    edgeGroupedGraph.triplets.map(triplet => triplet.srcAttr._1+" is the "+triplet.attr+" of "+triplet.dstAttr._1).collect().foreach { println(_) }

  }
}











