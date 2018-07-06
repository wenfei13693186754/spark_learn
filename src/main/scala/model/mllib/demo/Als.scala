package com.wdcloud.MLlib.demo

import java.util.logging.Level
import java.util.logging.Logger

import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

object Als{
  def main(args: Array[String]): Unit = {
    //屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARNING)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    
    //创建model和rating对象
    val modelObject = ModelOperate.createModel()  
    val model = modelObject._1
    val ratings = modelObject._2    
      
    //保存model中的用户和产品特征向量
    //val saveFeatures = ModelOperate.saveFeatures(model)
    
    //因为推荐出来的好友中包含了用户已经存在的好友，所以可以先获取该用户的所有好友，算出数量，然后在这个数量基础上加上要推荐的好友数量，这个和就是我们要推荐的好友的数量
    //然后从推荐的好友的中过滤掉用户已经有的好友，取剩余部分的前5个推荐给好友
    val userRating = ratings.filter { x => x.user==6 }.collect()
    val userFriendsCount = userRating.length
    
    val rec = model.recommendProducts(6,userFriendsCount+10)
    rec.foreach { x => println("给"+6+"过滤前推荐的好友是："+x)}
    
    //对推荐出来的好友进行过滤,去除掉用户自己和用户的好友
	 /*val recFriends = rec.filter { y => 
      (!(userRating.map { x => x.product }.contains(y.product)||y.product==6))  
    }
    recFriends.foreach { x => println("给"+6+"推荐的好友是："+x)}*/
    
    //**************************给物推荐人
    //val recPeoForItem = model.recommendUsers(14, 6).foreach { x => println("给"+14+"推荐的人是"+x) }
  }
 
}