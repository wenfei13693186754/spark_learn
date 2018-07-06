package com.wdcloud.graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import breeze.linalg.SparseVector
import scala.io.Source


/**
 * 推荐二度好友
 */
object RECFriends1 {
	val projectDir = "E:\\spark\\Spark-GraphX\\data\\recBasedGraph\\"
	val id = "1" //只建立这个ID对应的社交关系图
	//创建支持向量的二元搜索对象
	type Feature = breeze.linalg.SparseVector[Int]
	
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("graphDemo")
  	val sc = new SparkContext(conf)

    
//***********************创建用户属性向量*****************************************************
    
    //通过 .feat 文件,并将数据转化为由用户id和用户属性向量组成的map
    val featureMap = Source.fromFile(projectDir + id + ".feat").getLines().
      map {
        line =>
          val row = line.split(" ")
          //注意：ID 不能之间当作 Long 型的时候 常常用 hashcode 代替
          val key = row.head.toLong
          
          //SparseVector-->此辅助构造函数假定索引数组已经排序
          //tail:select all elements except the first one
          val feat = SparseVector(row.tail.map(_.toInt))
          (key, feat)
      }.toMap

//**************************创建图,图的每个边上放着用户之间的共同特征数*****************************************************
    
    //通过 .edges 文件得到两个用户之间的关系 并且计算他们相同特征的个数
    val edges = sc.textFile(projectDir + id + ".edges").map {
      line =>
        val row = line.split(" ")
        val srcId = row(0).toLong
        println(srcId)
        val dstId = row(1).toLong
        val srcFeat = featureMap(srcId)
        val dstFeat = featureMap(dstId)
        val numCommonFeats: Int = srcFeat dot dstFeat
        Edge(srcId, dstId, numCommonFeats)
    }
    //利用 fromEdges 建立图
    val edgeGraph = Graph.fromEdges(edges, 1).cache
    
    //查看一下具有3个相同特征的用户对
    print(edgeGraph.edges.filter(_.attr == 3).count())

//************************不考虑用户亲密度，只考虑一度好友所共有的二度好友的数量的推荐*************************************
    
	  println("*******************************************************")  
	  //找到userId对应的vertex和它对应的一度好友顶点以及它们之间的边
	  val oneTriplet = edgeGraph.triplets.filter(x => x.srcId == 1L)  
	  //输出user的一度好友
	  //oneTriplet.foreach(x => println(x.srcId+" 的一度好友是："+x.dstId))
	  //获取到一度好友的id
    val oneFriendsId = oneTriplet.map(x => x.dstId).collect()   
	   
    //找到user对应的二度好友顶点和它的源顶点以及它们之间的边
    val twoTriplet = edgeGraph.triplets.filter(x => oneFriendsId.contains(x.srcId) && x.dstId !=1)
    //找到二度好友的id
    val recFriends = twoTriplet.map(x => (x.dstId,1)).reduceByKey((x,y) => x+y).sortBy(x => x._2,false)
    recFriends.foreach { x => println("二度好友id是："+x._1+" 数量是："+x._2) }
    
//*******************考虑到用户之间亲密度的推荐，用用户之间的共有属性来表述用户之间的亲密度************************************
    
    //用户与二度好友亲密度计算
    //RDD[(VertexId, (VertexId, Int))]-->一度好友id,二度好友id,一度好友与二度好友亲密度
    val oneFriendsToTwoFriendsIntimacy = twoTriplet.map(x => (x.srcId,(x.dstId,x.attr)))
    //RDD[(VertexId, Int)]-->一度好友id,user和一度好友亲密度
    val userToOneFriendsIntimacy = oneTriplet.map(x => (x.dstId,x.attr))
    //RDD[(VertexId, ((VertexId, Int), Int))]-->RDD[(一度好友id,((二度好友id,一度好友和二度好友亲密度)，user和一度好友亲密度))]
    val mixIntimacy = oneFriendsToTwoFriendsIntimacy.join(userToOneFriendsIntimacy)
    //user与二度好友的亲密度
    //RDD[(VertexId, Int)]-->(二度好友id,亲密度)
    val initialIntimacy = mixIntimacy.map{x => 
      val intimacyData = x._2
      val oneFriendsToTwoFriendsIntimacy = intimacyData._1._2
      val userToOneFriendsIntimacy = intimacyData._2
      (intimacyData._1._1,oneFriendsToTwoFriendsIntimacy+userToOneFriendsIntimacy)
    }
    
    //因为建立起来的user和二度好友的亲密度可能会有多个值，原因就是某个二度好友可能被多个一度好友所拥有，这样就会被计算多次
    //当然也应该计算多次，但是要取多次计算的平均值作为该二度好友和user的亲密度
    val userToTwoFriendsIntimacy = initialIntimacy.reduceByKey((x,y) => (x+y))
    
    val numOfSameForTwoFriends = twoTriplet.map(x => (x.dstId,1)).reduceByKey((x,y) => x+y)
    val finRecResult = numOfSameForTwoFriends.join(userToTwoFriendsIntimacy).map{ x =>
      val intimacyData = x._2
      val finIntimacy = intimacyData._1*intimacyData._2
      (x._1,finIntimacy)
    }
    finRecResult.sortBy(x => x._2,false).collect().foreach { x => println("加入亲密度后二度好友id是："+x._1+" 亲密度是："+x._2) }
     
	} 
}




















