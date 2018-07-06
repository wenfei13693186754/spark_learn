package com.wdcloud.MLlib.recFriends

import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import com.wdcloud.MLlib.demo.Evalute


/**
 * 实现当有数据更新的时候,模型的同步更新
 * 1.sparkStreaming 实时获取数据，
 * 2.用新获取的数据更新模型
 * 3.用新的模型替换掉内存中旧的模型(删掉旧的模型，保存新的模型)
 * 4.持久化新的模型到HBase中，替换掉旧的模型
 */
class ModelUpdate {
  var oldModel:MatrixFactorizationModel = null;
  var oldRating:RDD[Rating] = null;
  var broadModel:Broadcast[MatrixFactorizationModel] = null;
  var broadRating:Broadcast[RDD[Rating]] = null;
  val conf = new SparkConf().setAppName("recommends").setMaster("spark://192.168.6.83:7077").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val sc = new SparkContext(conf) 
  def modelUpdate(): Unit = {  
    
    sc.addJar("E:\\friendsRec\\app.jar")
    
    //1.创建dStream对象，动态获取文件中的数据
    val ssc = new StreamingContext(sc,Seconds(1))
    val dStream = ssc.textFileStream("hdfs://192.168.6.83:9000/item.txt")
    //2.对数据进行处理
    val ratings = dStream.flatMap { line => 
      if(line.length()<=0){
    	  None
      }else{
    	  line.split(" ")
    	  match { case Array(user, item, num1, num2,num3)=>
    	  Some(user.toInt,item.toInt,(num1.toInt+num2.toInt+num2.toInt))}
      }
    }.map(x=>Rating(x._1,x._2,x._3)).cache()

    //3.用新数据更新模型
    ratings.foreachRDD{rating=>
      val b = 2
      println(b)
      val model = ALS.trainImplicit(rating, 12, 25,0.001,60.0)
      
      val a = model.productFeatures.mapValues { x => x.mkString(",") }.first()
      println("用户向量数量是："+a)
      val uc = oldModel.userFeatures.count()
      println("增加前userFeatures的数量是："+uc)
      val uf = oldModel.userFeatures.++(model.userFeatures)
      val pf = oldModel.productFeatures.++(model.productFeatures)
      val newModel = new MatrixFactorizationModel(12,uf,pf)
      
      //将新模型在内存中保存一份，在HBase中保存一份
      //保存到内存中
      broadModel = sc.broadcast(newModel)
      broadRating = sc.broadcast(rating)
      //保存到hdfs上
      rating.saveAsObjectFile("hdfs://192.168.6.83:9000/rating")
      newModel.save(sc, "hdfs://192.168.6.83:9000/model")
      //将特征向量保存下来
      newModel.userFeatures.map{ x => (x._1 + "\t" + x._2.mkString(",")) }.repartition(1).saveAsTextFile("hdfs://192.168.6.83:9000/userFeatures")
      newModel.productFeatures.map{ case (id, vec) => id + "\t" + vec.mkString }.repartition(1).saveAsTextFile("hdfs://192.168.6.83:9000/productFeatures")

      val addUc = newModel.userFeatures.count()
      println("增加后userFeatures的数量是："+addUc)
      val newRating = oldRating.++(rating)
    }
    //4.启动流计算环境StreamingContext,并等待它完成
    ssc.start()
    //5.等待作业完成
    ssc.awaitTermination()
  }
  
  /**
   * 创建模型
   */
  def createModel():(MatrixFactorizationModel,RDD[Rating])={
    //设置运行环境
    val modelInMemory = broadModel.value
    val ratingInMemory = broadRating.value
    if(modelInMemory==null){
 			val model = MatrixFactorizationModel.load(sc, "hdfs://192.168.6.83:9000//model")
 			val rating = sc.objectFile[Rating]("hdfs://192.168.5.83:9000/rating",3)
 			(model,rating)
    }else{
      (modelInMemory,ratingInMemory)
    }
  }
  
  /**
   * 对数据进行分区
   * 训练模型
   */
  def trainModel(ratings:RDD[Rating])={
    /*
     * 对数据进行分集
     * 将样本评分表以key值切分成3个部分，分别用于训练 (60%，并加入用户评分), 校验 (20%), and 测试 (20%)
   	 * 该数据在计算过程中要多次应用到，所以cache到内存
  	 *  
     */
    val numPartition = 4
    val splits = ratings.randomSplit(Array(0.6, 0.2,0.2), seed = 111l)
    val training = splits(0).repartition(numPartition).persist()
    val validation = splits(1).repartition(numPartition).persist()
    val test = splits(2).repartition(numPartition).persist()
    val numTraining = training.count()
    val numValidation = validation.count()
    val numTest = test.count()
    println("数据分区结果是：Training: " + numTraining + " validation: " + numValidation + " test: " + numTest)
    
    /*训练模型
  	 *调用ALS.train()方法，进行模型训练
  	 *ratings:RDD of(userID,productID,rating)pairs
  	 *rank:number of features to use使用特性的数量
  	 *iterations:number of iterations of ALS(recommended:10-20)
  	 *lambda:regularization factor(recommended:0.01)正则化因子
  	 *model:代表矩阵分解结果的模型
  	 */
  	 //2.训练不同参数下的模型，并在校验集中进行校验，获取最佳参数下的模型，
    val rank = List(8,12)
    val lambdas = List(0.1,10)
    val numIterations = List(10,20)
    var bestModel:Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    
    for(rank<-rank;lambda<-lambdas;numIteration<-numIterations){
      //trainImplicit(ratings: RDD[Rating], rank: Int, iterations: Int, lambda: Double, alpha: Double):
      val model = ALS.trainImplicit(training, rank, numIteration,0.01,1.0)  
      //validation：校验数据，numValidation:校验数据的数量
      //computeRmse:校验集预测数据和实际数据之间的均方根误差(标准差)（RMSE）
      val validationRmse = Evalute.computeMaeBySpark(model,validation)
      
      println("超参数"+"for the model trained with rank = "+rank+",lambda="+lambda+",and numIter="+numIteration+".")
     
      if(validationRmse<bestValidationRmse){
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIteration
      }
     }
  }
}