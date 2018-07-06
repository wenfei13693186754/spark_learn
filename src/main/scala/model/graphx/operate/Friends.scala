package com.wdcloud.graphx

import org.apache.spark.SparkConf
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.SparkContext

object Friends {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FriendsRecommend").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val graph = GraphLoader.edgeListFile(sc, "E:\\followers.txt").triplets
    // Use the triplets view to create an RDD of facts.
  /*  val facts = graph.map(triplet =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.collect.foreach(println(_))*/
    
  }  
}