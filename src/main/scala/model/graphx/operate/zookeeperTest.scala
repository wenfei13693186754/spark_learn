package com.wdcloud.Utils

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import model.mllib.mesPubSub.MessagePublicSubscribeZookeeper
import org.apache.spark.mllib.recommendation.Rating

object zookeeperTest {
  def main(args: Array[String]): Unit = {
    //设置运行环境
    val conf = new SparkConf().setAppName("recAPP")
    val sc = new SparkContext(conf) 
    val s0 = System.currentTimeMillis()
    
    MessagePublicSubscribeZookeeper.init()
    println("开始将model地址保存到zookeeper")
    val path = "hdfs://192.168.6.83:9000/evenNumberData"
    MessagePublicSubscribeZookeeper.createNode("/recModelAndRating", path)
	  println("保存完毕")
    
    //获取model存放路径
    println("从zookeeper上获取model地址")
    val mpsz = MessagePublicSubscribeZookeeper.getData("/recModelAndRating")
    println("获取成功，地址是："+mpsz)
    
    //获取model和rating对象
    val model = MatrixFactorizationModel.load(sc, mpsz+"/model")
		val ratings = sc.objectFile[Rating](mpsz+"/rating")
		println("对象获取成功")
  }
}