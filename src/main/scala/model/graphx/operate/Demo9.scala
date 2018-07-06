package com.wdcloud.graphx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

/*
 * 连接操作
 * 	joinVertices
 * 	outerJoinVertices
 */
object Demo9 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("graph").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val graph = GraphLoader.edgeListFile(sc, "E:\\spark\\Spark-GraphX\\data\\followers.txt")
    graph.triplets.collect().foreach(x => println("源顶点id是："+x.srcId+" 属性是： "+x.srcAttr+"  目的顶点id是："+x.dstId+" 目的顶点属性是： "+x.dstAttr+"  边属性是："+x.attr))
    
    //自定义pointTest点集
    val pointTest: RDD[(VertexId,Int)] = sc.parallelize(Array((1L,10),(2L,10),(3L,10),(5L,10)))
    //将graph顶点属性置为0,然后执行joinVertices操作
    val outputGraph = graph.mapVertices((id,attr) => 0).joinVertices(pointTest)((_,_,optDeg) => optDeg+1)
    outputGraph.triplets.collect.foreach(println(_))
    
    println("outerJoinVertices****************************************************")
    val outputGraphForOuterJoinVertices = graph.mapVertices((id,attr) => 0).outerJoinVertices(pointTest)((_,_,optDeg) => optDeg.getOrElse(0))
    outputGraphForOuterJoinVertices.triplets.foreach(x => println("源顶点id是："+x.srcId+" 属性是： "+x.srcAttr+"  目的顶点id是："+x.dstId+" 目的顶点属性是： "+x.dstAttr+"  边属性是："+x.attr))
    println("**********************************************************************************")
    //将顶点属性改为各点的出度
    val newOutputGraph = graph.mapVertices((id,attr) => 0).outerJoinVertices(pointTest)((_,_,optDeg) => optDeg) 
    newOutputGraph.triplets.foreach(x => println("源顶点id是："+x.srcId+" 属性是： "+x.srcAttr+"  目的顶点id是："+x.dstId+" 目的顶点属性是： "+x.dstAttr+"  边属性是："+x.attr))

    println("-------------------------------------")
    val outDeg = graph.outDegrees
    val graph1 = graph.joinVertices[Int](outDeg)((_, _, outDeg) => outDeg)
    graph1.triplets.collect().foreach(println)
  }
  
}