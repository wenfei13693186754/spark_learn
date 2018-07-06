package com.wdcloud.MLlib.recFriends

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import model.mllib.mesPubSub.MessagePublicSubscribeZookeeper


object ModelOperate{ 
  var mr : (MatrixFactorizationModel,RDD[Rating]) = null 
  /**
   * 创建模型
   */
  def main(args: Array[String]): Unit = {
	  //设置运行环境
    val conf = new SparkConf().setAppName("recAPP")
    val sc = new SparkContext(conf) 
    
    while(true){  
      var num = 0
      //1.获取文件中的数据
      val data = sc.textFile("hdfs://192.168.6.83:9000/user.txt")
      //2.对数据进行处理
      val rating = data.flatMap { line => 
        if(line==null||line.equals("")){
      	  None
        }else{  
      	  line.split("\\|")  
      	  match { case Array(user, item, num1, num2,num3)=>
      	  Some(user.toInt,item.toInt,(num1.toInt+num2.toInt+num2.toInt))}
        }
      }.map(x=>Rating(x._1,x._2,x._3)).cache()  
  
      val creStart = System.currentTimeMillis()
      //3.创建模型
      val model = ALS.trainImplicit(rating, 12, 25,0.001,60.0)
      val creEnd = System.currentTimeMillis()
      println("***********************创建model用时："+(creEnd-creStart)+"*********************************")
      
      
      try{
        if(num%2==0){
          //保存到hdfs上
          val s0 = System.currentTimeMillis()
          val path = "hdfs://192.168.6.83:9000/evenNumberData/"
      	  rating.saveAsObjectFile(path+"rating")
      	  model.save(sc, path+"/model")

//********将model和rating保存到内存中***********************************************************************************
      	  mr = (model,rating)
//********将地址保存到zookeeper上***************************************************************************************  
      	  println("开始将model地址保存到zookeeper")
      	  MessagePublicSubscribeZookeeper.init()  
          MessagePublicSubscribeZookeeper.createNode("/192.168.8.231", path)
      	  println("保存完毕")
//********************************************************************************************************************      	  
      	  val s1 = System.currentTimeMillis()
      	  println("***********************删除model用时："+(s1-s0)+"*********************************")
      	  //删除旧的model
      	  val delResult = deleteModel("hdfs://192.168.6.83:9000/oddNumberData")
      	  val delEndTime = System.currentTimeMillis()
      	  
        }else{ 
          //保存到hdfs上
      	  rating.saveAsObjectFile("hdfs://192.168.6.83:9000/oddNumberData/rating")
      	  model.save(sc, "hdfs://192.168.6.83:9000/oddNumberData/model") 
      	  //删除旧的model
    	    val delResult = deleteModel("hdfs://192.168.6.83:9000/evenNumberData")
        }
      }catch{
          case e:Exception => println("保存或者删除文件错误是："+e.getMessage+"**********")
      }
      
      if(num>10000){
        num = 0
      }else{
    	  num.+(1)
      }
      
      println("**********************************")
      //没隔一小时更新模型一次
      Thread.sleep(60*60*1000)
      println("当前线程是："+Thread.currentThread().getName)  
    }
  }
  
  
  /**
   * model的删除等操作
   */
  def deleteModel(path:String):Unit={ 
    //1.设置文件系统运行环境
    val conf = new Configuration()
    val hdfsCoreSitePath = new Path("core-site.xml")
    val hdfsHdfsSitePath = new Path("hdfs-site.xml")
    conf.addResource(hdfsCoreSitePath)
    conf.addResource(hdfsHdfsSitePath)
    
    //定位具体的hdfs有两种方式：一是在conf中配置，一个域名可以绑定多个ip,我们通过这个域名来定位hdfs;
    //另一种是在调用FileSystem.get时候指定一个域名或者一个ip,当然仅限一个
    
    //获取文件系统对象,这种方式是当hdfs的地址在配置文件配置了的情况下
    val fileSystem = FileSystem.get(conf)
    //下边是创建fs的时候指定hdfs地址
    //val fileSystem = FileSystem.get(URI.create("hdfs://192.168.6.83:9000"),conf)
    
    //操作hdfs中的文件
      var delRat = false
      var delModel = false
      delRat = fileSystem.delete(new Path(path+"/rating"))
      delModel = fileSystem.delete(new Path(path+"/model"))
    	fileSystem.close()
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
}