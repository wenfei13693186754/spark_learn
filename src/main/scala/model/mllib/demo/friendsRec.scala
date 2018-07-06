package com.wdcloud.MLlib.demo

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


/**
 * 进行好友推荐
 */
object friendsRec {
  
  val hbUtil = new hbaseUtils()
  
  /*
   * 对客户端传过来的一个或者多个用户id，进行批量推荐
   */
  def main(args: Array[String]):Unit={
    //设置运行环境
    //val conf = new SparkConf().setAppName("recAPP")
    //val sc = new SparkContext(conf)
    
    val s0 = System.currentTimeMillis()
    //给每个用户推荐的好友
    //val model = MatrixFactorizationModel.load(sc, "hdfs://192.168.6.83:9000/evenNumberData/model")
		//val ratings = sc.objectFile[Rating]("hdfs://192.168.6.83:9000/evenNumberData/rating")
    //创建model和rating对象
    val modelObject = ModelOperate.createModel()  
    val model = modelObject._1
    val ratings = modelObject._2 
		val s1 = System.currentTimeMillis()
		println("加载model用时："+(s1-s0)+"******开始推荐好友")
    val usersFriends = model.recommendProductsForUsers(10)
    val userAllFriends = usersFriends.map{x=>
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
      //hbUtil.saveRecResult("UIC_TEST",id.toString(),"YH","FRIENDS",recFriends)
    }
    val s2 = System.currentTimeMillis()
    println("推荐保存结束，用时："+(s2-s1))
  }
}