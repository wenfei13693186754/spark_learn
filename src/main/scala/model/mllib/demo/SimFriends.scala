package com.wdcloud.MLlib.demo

import java.util.logging.Level
import java.util.logging.Logger
import org.jblas.DoubleMatrix

/*
 * 计算出每个用户最相似的10个好友，并保存下来
 * 使用余弦相似度来计算
 */
object SimFriends {
  val hbUtil = new hbaseUtils()
  def main(args: Array[String]): Unit = {
    //屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARNING)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    
    //创建model和rating对象
    val modelObject = ModelOperate.createModel()  
    val model = modelObject._1
    val ratings = modelObject._2 
    
    val ids = ratings.map { x => x.user }.collect().par
    for(id <- ids){
    	val s0= System.currentTimeMillis()
    	println("开始给用户"+id+"找相似好友")
      
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