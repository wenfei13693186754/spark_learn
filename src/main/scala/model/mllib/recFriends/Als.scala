package com.wdcloud.MLlib.recFriends

import java.util.logging.Level
import java.util.logging.Logger
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.jblas.DoubleMatrix
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Als{
  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    println("***************开始时间是："+start+"*********************************")
    
    //设置运行环境
    val conf = new SparkConf().setAppName("recAPP")
    val sc = new SparkContext(conf) 
    //屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARNING)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    
    //创建model和rating对象
    val model = MatrixFactorizationModel.load(sc, "hdfs://192.168.6.83:9000/model")
		val ratings = sc.objectFile[Rating]("hdfs://192.168.6.83:9000/rating")

    //因为推荐出来的好友中包含了用户已经存在的好友，所以可以先获取该用户的所有好友，算出数量，然后在这个数量基础上加上要推荐的好友数量，这个和就是我们要推荐的好友的数量
    //然后从推荐的好友的中过滤掉用户已经有的好友，取剩余部分的前5个推荐给好友
    val userRating = ratings.filter { x => x.user==args(0)}.collect()     
    val userFriendsCount = userRating.length
    
    val rec = model.recommendProducts(args(0).toInt,userFriendsCount+10)   
    model
    //对推荐出来的好友进行过滤,去除掉用户自己和用户的好友
    val recFriends = rec.par.filter { y => 
      (!(userRating.map { x => x.product }.contains(y.product)||y.product==args(0)))  
    }
    recFriends.par.foreach { x => println("给"+args(0).toInt+"推荐的好友是："+x)}
    
    //**************************给物推荐人
    val recPeoForItem = model.recommendUsers(14, 6).par.foreach { x => println("给"+14+"推荐的人是"+x) }
    
    val stop = System.currentTimeMillis()
    println("***************结束时间是："+stop+"*********************************")
    val total = stop-start
    println("*************************总共用时："+total+"************************************************")
  }
 
  //通过计算两个商品之间的余弦相似度，来找出与已知商品最相似的商品
  def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }
}