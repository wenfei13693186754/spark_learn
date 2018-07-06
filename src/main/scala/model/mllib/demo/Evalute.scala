package com.wdcloud.MLlib.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.annotation.Since
import org.apache.spark.rdd.RDD.doubleRDDToDoubleRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/**
 * 使用spark内置的函数计算测试模型的准确度和推荐结果的准确度
 */
class Evalute {}
object Evalute{
  //MSE:平均绝对误差--所有单个观测值与算术平均值的偏差的绝对值的平均
  def computeMaeBySpark(model:MatrixFactorizationModel,ratings:RDD[Rating]):Double={
    
    val usersProducts= ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }

    var predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
    }

    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)
    
    val predictedAndTrue = ratesAndPreds.map { case ((user, product), (actual, predicted)) => (actual, predicted) }
    val regressionMetrics = new RegressionMetrics(predictedAndTrue)
    //meanSquaredError:返回平均绝对误差----
    //rootMeanSquaredError：返回均方根误差,定义为均方误差的平方根。
    println("Mean Squared Error = " + regressionMetrics.meanSquaredError)
    println("Root Mean Squared Error = " + regressionMetrics.rootMeanSquaredError)
    
    regressionMetrics.meanSquaredError
  }
  
     /**
   * 计算RMSE,用来预测模型预测的准确度,适合于显性反馈数据训练出来的模型的判断
   * 值越小，模型越精确
   * 均方根误差就是标准差，可以用来推测数据的离散程度 
   */
  def computeRmse(model:MatrixFactorizationModel,data:RDD[Rating])={
    //获取用户商品映射
    val userProducts = data.map { case Rating(user,product,rate) => (user,product) }
    userProducts.foreach(x=>println("用户产品映射是："+x._1+"::"+x._2))
    //使用推荐模型对用户和好友的相似度进行预测评分
    val predictions = model.predict(userProducts).map { case Rating(user,product,rate) => ((user,product),rate) }
    predictions.foreach(x=>println("预测得分"+x._1+"：：："+x._2))
    //获取用户对好友的实际评分
    val realRates = data.map { case Rating(user,product,rate) => ((user,product),rate) }
    realRates.foreach(x=>println("用户的实际评分是："+x._1+"::"+x._2))
    //将真实相似度评分和预测相似度评分进行合并
    val ratesAndPreds = realRates.join(predictions)
    //计算根均方差(RMSE),用来判断模型的预测相似度评分和实际评分的接近程度
    val rmse = math.sqrt(ratesAndPreds.map{case ((user,product),(r1,r2))=>
        val err = (r1-r2)
        err*err
    }.mean())
    println("******模型预测的准确度："+s"RMSE = $rmse")
  
    //保存实际评分和预测评分  
    //上面这段代码先按用户排序，然后重新分区确保目标目录中只生成一个文件。如果你重复运行这段代码，则需要先删除目标路径：
    /*ratesAndPreds.sortByKey().repartition(1).sortBy(_._1).map({
    case ((user, product), (rate, pred)) => (user + "," + product + "," + rate + "," + pred)
    }).saveAsTextFile("E:\\friendsRec\\result.txt")*/
    rmse
  
  }
}