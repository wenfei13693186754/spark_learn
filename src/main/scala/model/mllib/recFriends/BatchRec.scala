package com.wdcloud.MLlib.recFriends

import java.io.Serializable

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/***
 * 实现批量推荐
 * 使用spark提供的批量推荐API进行推荐；
 * 将推荐出的结果剔除用户自己和他已有的好友，形成一个数据集
 * 将数据集保存到HBase上
 */
object BatchRec extends Serializable{
  def main(args: Array[String]):Unit={  
    //设置运行环境
    val conf = new SparkConf().setAppName("recAPP")
    val sc = new SparkContext(conf) 
    val s0 = System.currentTimeMillis()
    println("加载model-------------------------------")
    
    //获取model存放路径
    //println("从zookeeper上获取model地址")
    //val mpsz = MessagePublicSubscribeZookeeper.getData("/192.168.8.231")
   //println("获取成功，地址是："+mpsz)
    
    //获取model和rating对象
    //val model = MatrixFactorizationModel.load(sc, mpsz+"model")
		//val ratings = sc.objectFile[Rating](mpsz+"rating")
    val model = MatrixFactorizationModel.load(sc, "hdfs://192.168.6.83:9000/evenNumberData/model")
		val ratings = sc.objectFile[Rating]("hdfs://192.168.6.83:9000/evenNumberData/rating")
		val s1 = System.currentTimeMillis()
		println("加载model用时："+(s1-s0))
		
		val s2 = System.currentTimeMillis()
    println("开始推荐好友---------------------")
		//给每个用户推荐的好友
    val usersFriends = model.recommendProductsForUsers(args(0).toInt)
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
      val uf = (y._1.diff(y._2)).take(10)
      (x._1,uf)  
    }
    val s3 = System.currentTimeMillis()
    println("推荐好友用时："+(s3-s2)+"-----开始保存数据")
    
    //将提前预测出来的数据保存下来
    usersfriends.foreach{x=>
      val id = x._1
      val recFriends = x._2
      saveRecResult("UIC_TEST",id.toString(),"YH","FRIENDS",recFriends)
      println("保存成功----------------")
    }
    val s4 = System.currentTimeMillis()
    println("保存model用时："+"s4-s3")
    println("总共用时："+(s4-s0))
    
  }

  /**
   * write
   */
  def saveRecResult(tableName: String, rowKey:String,familyName: String, columnName: String,data: Array[Int]): Unit = {
    //write("UIC_TEST", "002", "YH", "FRIENDS")
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "namenode,datanode1,datanode2,datanode3");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.set("hbase.defaults.for.version.skip", "true")
    val table = new HTable(conf, Bytes.toBytes(tableName));
    val put = new Put(Bytes.toBytes(rowKey))  
    put.addColumn(familyName.getBytes(), columnName.getBytes(), Bytes.toBytes(data.mkString(",")));
    table.put(put)
    table.close();
  }
}