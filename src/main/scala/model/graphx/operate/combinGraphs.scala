package model.graphx.operate

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
/**
 * 将多个图合并为一个图
 */
object combinGraphs {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("demo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    //创建图1
    val vertex1 = sc.parallelize(Seq(
      (1L,"A"),
      (2L,"B"),
      (3L,"C"),
      (4L,"D"),
      (5L,"E"),
      (6L,"F")
    ))
    
    val edge1 = sc.parallelize(Seq(
      Edge(1L,2L,1),
      Edge(2L,3L,1),
      Edge(3L,4L,1),
      Edge(4L,5L,1),
      Edge(5L,6L,1)
    ))
    
    val graph1 = Graph(vertex1, edge1)
    
    //创建图2
    val vertex2 = sc.parallelize(Seq(
        (1L,"A"),
        (2L,"B"),
        (3L,"C"),
        (7L,"G")))
    val edge2 = sc.parallelize(Seq(
        Edge(1L,2L,1),
        Edge(2L,3L,1),
        Edge(1L,7L,1)))
    val graph2 = Graph(vertex2, edge2)
    
    //合并两个图
    val graph: Graph[String, Int] = Graph(
        graph1.vertices.union(graph2.vertices),
        graph1.edges.union(graph2.edges)    
    ).partitionBy(RandomVertexCut).groupEdges((attr1,attr2) => attr1 + attr2)

    graph.triplets.collect().foreach(println)
  }
}