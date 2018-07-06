package com.wdcloud.MLlib.recBasedLabel

import scala.util.Try

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import com.typesafe.config.Config

import spark.jobserver.SparkJob
import spark.jobserver.SparkJobInvalid
import spark.jobserver.SparkJobValid
import spark.jobserver.SparkJobValidation
import org.jblas.DoubleMatrix
import breeze.macros.expand.args
import org.apache.spark.storage.StorageLevel
import breeze.macros.expand.args

object similarityRank3 extends SparkJob{
   override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("input.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No input.string config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    val md = sc.textFile("/simTestFile/"+"user1.txt")
    
    val movieData = md.map { x =>
      val array = x.split("\\|")
      val movieLabel = array(1).split(" ").map { x => x.toDouble }  
      (array(0),movieLabel)
    }.persist(StorageLevel.MEMORY_AND_DISK_SER) 
    
    
    val ud = sc.textFile("/simTestFile/userMovie.txt",4)

    val userData = ud.map { x => x.split("\\|") }.filter(x=>x(0)=="12")
    //得到user对movie的喜好标签user profile
    val userLike = userData.map { x => x(1).split(" ") }.first()
    val userProfile = userLike.map { x => x.toDouble }
    val itemVector = new DoubleMatrix(userProfile)
    
    val simArr = movieData.map{x =>  
      val factorVector = new DoubleMatrix(x._2)  
      val sim = cosineSimilarity(factorVector,itemVector)
      //自定义的排序方法
      (x._1,sim)
    }

    //sortBy(),查看源码可知它默认是升序排列的
    val data = simArr.sortBy(x => x._2).take(11)
    data.foreach(x =>println("("+x._1+"|"+x._2+")"))
    data
  }
  
  import org.jblas.DoubleMatrix
  /* Compute the cosine similarity between two vectors */
  def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2()+0.001)
  }
}
  