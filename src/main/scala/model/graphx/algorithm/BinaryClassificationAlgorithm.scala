package com.wdcloud.Utils

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/**
 * 二分类
 * 在很多场景下都能碰到反应变量为二分类的资料，如考察公司中总裁级的领导层中是否有女性员工，某一天是否下雨，某患病者是否痊愈，
 * 调查对象是否为某商品的潜在消费者等
 * AUC:就是计算ROC曲线下区域的面积就是AUC
 */
object BinaryClassificationAlgorithm {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AUC").setMaster("local[*]")
    val sc = new SparkContext(conf)
     // Load training data in LIBSVM format：将数据加载为LIBSVM类型数据
    val data = MLUtils.loadLibSVMFile(sc, "C:\\Users\\Administrator\\Desktop\\examples\\sample_binary_classification_data.txt")
    val a = data.collect()
    // Split data into training (60%) and test (40%)：数据分为训练集和测试集
    val Array(training, test) = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    training.cache()
    
    // Run training algorithm to build the model：运行训练集 创建model
    val model = new LogisticRegressionWithLBFGS()
      //Set the number of possible outcomes for k classes classification problem in Multinomial Logistic Regression.
      //在多项逻辑回归中的K类分类问题的可能结果的集合。
      .setNumClasses(2)
      .run(training)
    
    // Clear the prediction threshold so the model will return probabilities
    //清除预测阈值，这样模型将返回概率
    model.clearThreshold
    
    // Compute raw scores on the test set:在测试集上计算行得分
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }
    
    // Instantiate metrics object实例化度量对象
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    
    // Returns the (threshold, precision) curve：返回（阈值、精度）曲线
    val precision = metrics.precisionByThreshold
    precision.foreach { case (t, p) =>
      println(s"Threshold: $t, Precision: $p")
    }
    
    // Recall by threshold
    //返回(阈值,召回)曲线。
    val recall = metrics.recallByThreshold
    recall.foreach { case (t, r) =>
      println(s"Threshold: $t, Recall: $r")
    }
    
    // Precision-Recall Curve
    //Returns the precision-recall curve, which is an RDD of (recall, precision), NOT (precision, recall), with (0.0, 1.0) prepended to it.
    //返回precision-recall曲线,这是一个RDD(记得,精度),而不是(精度、召回),(0.0,1.0)返回它。
    val PRC = metrics.pr
    
    // F-measure
    //Returns the (threshold, F-Measure) curve.
    val f1Score = metrics.fMeasureByThreshold
    f1Score.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 1")
    }
    
    val beta = 0.5
    val fScore = metrics.fMeasureByThreshold(beta)
    f1Score.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 0.5")
    }
    
    // AUPRC
    //Computes the area under the precision-recall curve.
    val auPRC = metrics.areaUnderPR
    println("Area under precision-recall curve = " + auPRC)
    
    // Compute thresholds used in ROC and PR curves
    val thresholds = precision.map(_._1)
    
    // ROC Curve
    val roc = metrics.roc
    
    // AUROC
    val auROC = metrics.areaUnderROC
    println("Area under ROC = " + auROC)
  }
 
}