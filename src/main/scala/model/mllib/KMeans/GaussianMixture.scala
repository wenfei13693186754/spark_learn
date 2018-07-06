package com.wdcloud.MLlib.KMeans

import org.apache.spark.mllib.clustering.{GaussianMixture, GaussianMixtureModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object  GaussianMixture {
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("KMeans").setMaster("local[*]")
  	val sc = new SparkContext(conf)
    
    // Load and parse the data
    val data = sc.textFile("E:\\Spark-MLlib\\data\\gmm_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble))).cache()
    
    // Cluster the data into two classes using GaussianMixture
    val gmm = new GaussianMixture().setK(2).run(parsedData)
    
    // Save and load model
    //gmm.save(sc, "target/org/apache/spark/GaussianMixtureExample/GaussianMixtureModel")
    //val sameModel = GaussianMixtureModel.load(sc,
      //"target/org/apache/spark/GaussianMixtureExample/GaussianMixtureModel")
    
    // output parameters of max-likelihood model
    for (i <- 0 until gmm.k) {
      println("weight=%f\nmu=%s\nsigma=\n%s\n" format
        (gmm.weights(i), gmm.gaussians(i).mu, gmm.gaussians(i).sigma))
    }
  }
}