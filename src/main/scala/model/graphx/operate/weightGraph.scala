package com.wdcloud.graphx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import breeze.linalg.SparseVector
import scala.io.Source
import org.apache.spark.graphx.Edge


/**
 * 创建人与人之间相似性的权重图
 */


object weightGraph {
	val projectDir = "E:\\spark\\Spark-GraphX\\data\\gplus\\gplus\\"
	val id = "100129275726588145876" //只建立这个ID对应的社交关系图
	//创建支持向量的二元搜索对象
	type Feature = breeze.linalg.SparseVector[Int]
	
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkInAction").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //通过 .feat 文件,并将数据转化为由用户id和用户属性向量组成的map
    val featureMap = Source.fromFile(projectDir + id + ".feat").getLines().
    map {
      line =>
        val row = line.split(" ")
        //注意：ID 不能之间当作 Long 型的时候 常常用 hashcode 代替
        val key = Math.abs(row.head.hashCode.toLong)
        
        //SparseVector-->此辅助构造函数假定索引数组已经排序
        //tail:select all elements except the first one
        val feat = SparseVector(row.tail.map(_.toInt))
        (key, feat)
    }.toMap
  
    //通过 .edges 文件得到两个用户之间的关系 并且计算他们相同特征的个数
    val edges = sc.textFile(projectDir + id + ".edges").map {
      line =>
        val row = line.split(" ")
        val srcId = Math.abs(row(0).hashCode.toLong)
        val dstId = Math.abs(row(1).hashCode.toLong)
        val srcFeat = featureMap(srcId)
        val dstFeat = featureMap(dstId)
        val numCommonFeats: Int = srcFeat dot dstFeat
        Edge(srcId, dstId, numCommonFeats)
    }
  
    //利用 fromEdges 建立图
    val egoNetwork = Graph.fromEdges(edges, 1)
    
    //查看一下具有3个相同特征的用户对
    print(egoNetwork.edges.filter(_.attr == 3).count())
    }
}