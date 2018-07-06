package com.wdcloud.MLlib.demo
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.TableName
import scala.collection.JavaConverters.asScalaIteratorConverter

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.BinaryComparator
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.filter.DependentColumnFilter
import org.apache.hadoop.hbase.filter.FamilyFilter
import org.apache.hadoop.hbase.filter.QualifierFilter
import org.apache.hadoop.hbase.filter.RowFilter
import org.apache.hadoop.hbase.filter.SubstringComparator
import org.apache.hadoop.hbase.filter.ValueFilter
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import java.io.Serializable
import org.apache.spark.serializer.KryoRegistrator
import org.apache.hadoop.hbase.client.Get
import java.util.logging.Level
import java.util.logging.Logger
import org.jblas.DoubleMatrix

/***
 * 将给每个用户要推荐的好友和该用户的相似好友保存到HBase中
 */
class userFriendsSave {
  
  val hbUtil = new hbaseUtils()
  //创建model和rating对象
  val modelObject = ModelOperate.createModel()  
  val model = modelObject._1
  val ratings = modelObject._2
  
  /*
   * 将给用户要推荐的好友保存到HBase上
   */
  def recFriendsAndSave(args: Array[String]):Unit={    

    //给每个用户推荐的好友
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
      hbUtil.saveRecResult("UIC_TEST",id.toString(),"YH","FRIENDS",recFriends)
    }
  }
  
  /*
   * 获取到每个用户的最相似的10个好友，保存下来
   */
  def simsFriendsAndSave(): Unit = {
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