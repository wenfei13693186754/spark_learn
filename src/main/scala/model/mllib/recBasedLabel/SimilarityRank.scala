package com.wdcloud.MLlib.recBasedLabel

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.jblas.DoubleMatrix
import scala.util.control.Breaks._
import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.collection.mutable.HashMap
import org.apache.spark.Accumulable


object SimilarityRank {
	val conf = new SparkConf().setAppName("recBasedLabel").setMaster("local[*]")
	val sc = new SparkContext(conf)
  //def simItemRec(userProfile:Array[Double]):HashMap[String, Double] = {
  def main(args: Array[String]): Unit = {
	  /*
	   *  0.0,0.0,0.0,1.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
        0.0,0.0,0.0,1.0,1.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
        0.0,0.0,0.0,1.0,1.0,1.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
        0.0,0.0,0.0,1.0,1.0,1.0,1.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
        0.0,0.0,0.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
        0.0,0.0,0.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
	   */
	  val arr1 = Array(0.0,0.0,0.0,1.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0)
	  val arr2 = Array(0.0,0.0,0.0,1.0,1.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0)
	  val arr3 = Array(0.0,0.0,0.0,1.0,1.0,1.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0)
	  val arr4 = Array(0.0,0.0,0.0,1.0,1.0,1.0,1.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0)
	  val arr5 = Array(0.0,0.0,0.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0)
	  val arr6 = Array(0.0,0.0,0.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0)
	  val f1 = new DoubleMatrix(arr1)  
	  val f2 = new DoubleMatrix(arr2)
	  val f3 = new DoubleMatrix(arr3)  
	  val f4 = new DoubleMatrix(arr4)
	  val f5 = new DoubleMatrix(arr5)  
	  val f6 = new DoubleMatrix(arr6)
	  val num1 = cosineSimilarity(f1, f2)
	  val num2 = cosineSimilarity(f1, f3)
	  val num3 = cosineSimilarity(f1, f4)
	  val num4 = cosineSimilarity(f1, f5)
	  val num5 = cosineSimilarity(f1, f6)
	  println("相似度："+num1+" "+num2+" "+num3+" "+num4+" "+num5)
	  
	  
	  
	  
    //读取数据，数据格式：4|0 1 1 0-----分别代表用户id,电影的属性
   /* val movieData = sc.textFile("E:\\Spark-MLlib\\data\\user5.txt",4).map { x => 
      val array = x.split("\\|")
      val movieLabel = array(1).split(" ").map { x => x.toDouble }  
      (array(0),movieLabel)
    }
    val userData = sc.textFile("E:\\Spark-MLlib\\data\\dataBasedLabel\\userMovie.txt",4).map { x => x.split("\\|") }.filter(x=>x(0)=="12")
    //得到user对movie的喜好标签user profile
    val userLike = userData.map { x => x(1).split(" ") }.first()
    val userProfile = userLike.map { x => x.toDouble }
    val itemVector = new DoubleMatrix(userProfile)
    
    //初始化MutableHashMap
    val map = MutableHashMap[String,Double]("1" -> -1.1,"2" -> -1.2,"3" -> -1.3,"4" -> -1.4,"5" -> -1.5)
    var accumulator: Accumulable[MutableHashMap[String, Double], (String, Double)] = sc.accumulable(map)(new HashMapParam)  
    accumulator +=("1"->0.1)  
    println(accumulator.localValue.get("5")) 
    println("当前线程是："+Thread.currentThread()+"---------"+accumulator.localValue.get("5"))
         
    val al = movieData.map{x =>  
      val factorVector = new DoubleMatrix(x._2)  
      val sim = cosineSimilarity(factorVector,itemVector)
      //自定义的排序方法
      println("当前线程是："+Thread.currentThread().getName)
      accumulator +=("1"->0.1) 
      accumulator.localValue
    }   
    println(al.count())*/
  }     
  
	
  def userProfile(id:String):Array[Double] = {
    //读取数据，数据格式：0|11,12,13,18,14|20,21,22-----分别代表用户id,用户最近看过的书籍id和用户喜好书籍的标签id
    val userData = sc.textFile("E:\\Spark-MLlib\\data\\dataBasedLabel\\userMovie.txt",4).map { x => x.split("\\|") }.filter(x=>x(0)==id)
    //得到user对movie的喜好标签user profile
    val userLike = userData.map { x => x(1).split(" ") }.first()
    val userProfile = userLike.map { x => x.toDouble }
    userProfile
  }
  
  
  import org.jblas.DoubleMatrix
  /* Compute the cosine similarity between two vectors */
  def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2()+0.1)
  }
}