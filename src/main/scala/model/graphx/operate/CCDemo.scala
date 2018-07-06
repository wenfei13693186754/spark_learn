package model.graphx.operate

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CCDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("cc").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // Load the graph as in the PageRank example
    val graph = GraphLoader.edgeListFile(sc, "E:\\followers.txt")
    // Find the connected components
    val cc = graph.connectedComponents().vertices
    // Join the connected components with the usernames
    val users = sc.textFile("E:\\users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }
    // Print the result
    println(ccByUsername.collect().mkString("\n"))
  }
}