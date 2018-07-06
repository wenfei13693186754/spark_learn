package com.wdcloud.MLlib.KMeans

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

object KMeansExample2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KMeans").setMaster("local[*]")
  	val sc = new SparkContext(conf)
    // Load and parse the data
    val data = sc.textFile("E:\\Spark-MLlib\\data\\kmeans_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()
    
    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    
    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)
    
    // Save and load model
    clusters.save(sc, "E:\\Spark-MLlib\\data\\KMeansModel")
    val sameModel = KMeansModel.load(sc, "E:\\Spark-MLlib\\data\\KMeansModel")
  }
}