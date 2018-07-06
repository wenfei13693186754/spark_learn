package com.wdcloud.MLlib.recFriends

import java.util.logging.Level
import java.util.logging.Logger
import org.jblas.DoubleMatrix
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*
 * 计算出每个用户最相似的10个好友，并保存下来
 * 使用余弦相似度来计算
 */
object SimFriendsLinux {
  def main(args: Array[String]): Unit = {
	  val hbUtil = new hbaseUtilsLinux()
    //设置运行环境
    val conf = new SparkConf().setAppName("recAPP")
    val sc = new SparkContext(conf) 
    //创建model和rating对象
    val model = MatrixFactorizationModel.load(sc, "hdfs://192.168.6.83:9000/evenNumberData/model")
		val ratings = sc.objectFile[Rating]("hdfs://192.168.6.83:9000/evenNumberData/rating")
    val usersArray = collection.mutable.ArrayBuffer[(String,Array[String])]()
    val ids = ratings.map { x => x.user }.collect().par
    for(id <- ids){
    	val s0= System.currentTimeMillis()
    	println("开始给用户"+id+"找相似好友")    
      
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
      println("开始保存数据")
      hbUtil.saveRecResult("UIC_TEST",id.toString(),"YH","SIMSFRIENDS",simsFriends)
      println("保存成功")
      val s1 = System.currentTimeMillis()
      println("好友找到,总共用时："+(s1-s0))
    }
  }
  
  import org.jblas.DoubleMatrix
  /* Compute the cosine similarity between two vectors */
  def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }

}