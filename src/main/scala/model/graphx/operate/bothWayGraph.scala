package model.graphx.operate

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader


  /**
   * 创建一个双向图
   * 数据集合类型是：
   * 	1	66
      1	67
      1	68
      1	69
    所以很适合使用edgeListFile方法创建图形
 */

object bothWayGraph {
	
  val conf = new SparkConf().setMaster("local[*]").setAppName("graphDemo")
	val sc = new SparkContext(conf)
  
	def main(args: Array[String]): Unit = {

    //创建图
    val emailGraph = GraphLoader.edgeListFile(sc, "E:\\spark\\Spark-GraphX\\data\\email-Enron.txt\\Email-Enron.txt")
    //查看图中前面5个顶点
    emailGraph.vertices.take(5).foreach(println)
    //查看图的前边五条边
    emailGraph.edges.take(5).foreach { x => println(x) }
    println("**************************************")
    //查看顶点19021是否是双向图
    emailGraph.edges.filter { x => x.srcId == 19021 }.map { x => x.dstId }.foreach { println }
    println("***************************************")
    emailGraph.edges.filter { x => x.dstId == 19021 }.map { x => x.srcId }.foreach { println }
  }
  
  
  
  
}