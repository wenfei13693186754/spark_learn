package com.wdcloud.MLlib.demo

import java.util.logging.Level
import java.util.logging.Logger

import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.jblas.DoubleMatrix

/**
 * 物品的推荐
 *  1.以人推物(啤酒尿布 的实践)
 *  	和用户相似的好友喜欢的物品也可能是该用户喜欢的，从而进行推荐
 *  2.基于物物相似度的推荐
 */
object itemRecAndSave {
  val hbUtil = new hbaseUtils()
  //屏蔽不必要的日志显示在终端上
  Logger.getLogger("org.apache.spark").setLevel(Level.WARNING)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  
  //创建model和rating对象
  val modelObject = ModelOperate.createModel()  
  val model = modelObject._1
  val ratings = modelObject._2    
  def main(args: Array[String]): Unit = {

  }
  
  def recItemAndSave(args: Array[String]):Unit={    

    //给每个用户推荐的好友
    val usersItems = model.recommendProductsForUsers(10)
    val userAllFriends = usersItems.map{x=>
      val userFriends = x._2.map { x => x.product }.toArray
      (x._1,userFriends)
    }.cache()  
    
    //获取到每个用户和他对应的好友
    val userRatings = ratings.groupBy { x => x.user }  
    val userToFriends = userRatings.map{x=>
      val userFriends = x._2.map { x => x.product }.toArray
      (x._1,userFriends)    
    }.cache()   
    //**********先进行并集操作，然后进行差集操作
    val nds = userAllFriends.join(userToFriends)
    val usersfriends = nds.map{x=>
      val y = x._2
      val uf = (y._1.diff(y._2))
      val ufs = uf.take(10)
      (x._1,ufs)  
    }
    
    usersfriends.foreach{x=>
      val id = x._1
      val recFriends = x._2
      hbUtil.saveRecResult("UIC_TEST",id.toString(),"YH","FRIENDS",recFriends)
    }
  }
  
  /*
   * 获取到每个用户的最相似的10个好友，保存下来
   */
  def simsItemAndSave(): Unit = {
    println("开始")
    val s0 = System.currentTimeMillis()
    val usersArray = collection.mutable.ArrayBuffer[(String,Array[String])]()
    val ids = ratings.map { x => x.user }.collect().par
    for(id <- ids){
      println(id)
      val uf = model.userFeatures.lookup(id).head
      val itemVector = new DoubleMatrix(uf)
      
      val sims = model.userFeatures.map{ case(id,factor) => 
        val factorVector = new DoubleMatrix(factor)
        val sim = cosineSimilarity(factorVector,itemVector)
        (id,sim)
      }
      
      val sortedSims = sims.top(11)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
      val simsFriends = sortedSims.slice(1, 11).map(x=>x._1)
      //将和用户相似的好友保存在集合中
      hbUtil.saveRecResult("UIC_TEST",id.toString(),"YH","SIMSFRIENDS",simsFriends)
    }
    val s1 = System.currentTimeMillis()
    println("结束,总共用时："+(s1-s0))
  }
  
  import org.jblas.DoubleMatrix
  /* Compute the cosine similarity between two vectors */
  def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }
}