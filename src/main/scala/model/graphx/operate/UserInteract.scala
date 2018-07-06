package com.wdcloud.graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.EdgeRDDImpl

/**
 * 用户之间互动，比如点赞，互相分享信息，评论，评价，评论，回复等信息
 * 导致的产生的用户之间的双边关系
 * A————点赞—————>B
 * A——— 评价—————>B
 * (srcId,赞，dstId)
 */
object UserInteract {
	val graph = RecFriends2.createGraph()
  val conf = new SparkConf().setMaster("local[*]").setAppName("graphDemo")
	val sc = new SparkContext(conf)
  def main(args: Array[String]): Unit = {
	  
  }
  
  /*
   * 处理用户之间点赞行为
   * 当A点赞B的时候，在图中的A和B顶点之间产生由A指向B的边，边上的属性是点赞
   * 对于用户之间没有交集的情况下，用户之间没有边来表示这层关系，所以需要创建一条边
   */
  def interactDeal (srcId: Long, attr: Array[Int], dstId: Long) {
    val addEdge = Edge(srcId, dstId, attr)
    //val a = graph.edges.innerJoin()  
    graph.mapEdges(x => 
      if (x.srcId == srcId && x.dstId == dstId) {
        //x.attr.:+(attr)
      }else {
        attr
      }  
    )
  }
}