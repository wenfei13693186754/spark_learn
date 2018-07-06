package com.wdcloud.MLlib.demo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed._ 

class ModelOperate private() {}

object ModelOperate extends Serializable{ 
	var mr : (MatrixFactorizationModel,RDD[Rating]) = null 
  /**
   * 创建模型
   * 实现模型的定时更新
   */
  def createModel():(MatrixFactorizationModel,RDD[Rating])={
    //设置运行环境
    val conf = new SparkConf().setAppName("recAPP").setMaster("local[*]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf) 
    //1.获取文件中的数据
    val data = sc.textFile("E:\\Spark-MLlib\\data\\item.txt")
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

    //3.用新数据更新模型 
    val model = ALS.trainImplicit(rating, 12, 25,0.001,60.0)
		
    (model,rating)  
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