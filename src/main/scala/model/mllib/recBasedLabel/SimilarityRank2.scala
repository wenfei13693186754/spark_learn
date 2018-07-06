package com.wdcloud.MLlib.recBasedLabel

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.jblas.DoubleMatrix

class SimilarityRank2 {
  val conf = new SparkConf().setAppName("recBasedLabel").setMaster("spark://192.168.6.83:7077")
  //val conf = new SparkConf().setAppName("recBasedLabel").setMaster("local[*]")
	val sc = new SparkContext(conf)
  def simItemRec(args:Array[String]):Array[(String, Double)] = {
  //def main(args: Array[String]): Unit = {
    val t3 = System.currentTimeMillis()
    println("开始获取数据并处理")
    //读取数据，数据格式：4|0 1 1 0-----分别代表用户id,电影的属性  /simTestFile/user.txt
    val md = sc.textFile("/simTestFile/"+args(0))
    //val md = sc.textFile("E:\\Spark-MLlib\\data\\aa.txt",4)
    
    val movieData = md.map { x =>
      val array = x.split("\\|")
      val movieLabel = array(1).split(" ").map { x => x.toDouble }  
      (array(0),movieLabel)
    }.persist(StorageLevel.MEMORY_AND_DISK_SER) 
    
    
    val ud = sc.textFile("/simTestFile/userMovie.txt",4)
    //val ud = sc.textFile("E:\\Spark-MLlib\\data\\dataBasedLabel\\userMovie.txt",4)

    val userData = ud.map { x => x.split("\\|") }.filter(x=>x(0)==args(1))
    //得到user对movie的喜好标签user profile
    val userLike = userData.map { x => x(1).split(" ") }.first()
    val userProfile = userLike.map { x => x.toDouble }
    val itemVector = new DoubleMatrix(userProfile)
    
//*****余弦相似度*********************************************************************    
    val t2 = System.currentTimeMillis()
    println("数据获取处理完成，用时："+(t2-t3)+"----开始使用余弦相似度计算相似度")
    val simArr = movieData.map{x =>  
      val factorVector = new DoubleMatrix(x._2)  
      val sim = cosineSimilarity(factorVector,itemVector)
      //自定义的排序方法
      (x._1,sim)
    }
    simArr.count
    val t0 = System.currentTimeMillis()
    println("相似度计算用时："+(t0-t2)+"---开始排序")
    
//***欧式距离**********************************************************************    
/*    val t2 = System.currentTimeMillis()
    println("数据获取处理完成，用时："+(t2-t3)+"----开始使用欧式距离计算相似度")
    val simArr = movieData.map{x =>  
      val euc = euclidean.eucl(x._2,userProfile)
      (x._1,euc)
    }
    simArr.count
    val t0 = System.currentTimeMillis()
    println("相似度计算用时："+(t0-t2)+"---开始排序")*/
//********************************************************************************     
    //sortBy(),查看源码可知它默认是升序排列的
    val data = simArr.sortBy(x => x._2).take(11)
    val t1 = System.currentTimeMillis()
    println("排序用时"+(t1-t0))
    data.foreach(x =>println("("+x._1+"|"+x._2+")"))
    println("总共用时"+(t1-t3))
    data
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
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2()+0.001)
  }
}