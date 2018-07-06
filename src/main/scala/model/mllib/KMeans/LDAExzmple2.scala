package com.wdcloud.MLlib.KMeans

import org.apache.spark.ml.clustering.LDA
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object LDAExzmple2 {
  def main(args: Array[String]): Unit = {
    /*val conf = new SparkConf()
    val sc = new SparkContext(conf)
    //load data
    val dataset = spark.read.format("libsvm").load("")
    
    //Train a LDA model
    val lda = new LDA().setK(10).setMaxIter(10)
    val model = lda.fit(dataset)
    
    val ll = model.logLikeLihood(dataset)
    val lp = model.logPerplexity(dataset)
    
    //语料库中的这篇log的下边界可能是：。。。
    println(s"The lower bound on the log likelihood of entire corpus:$ll")
    //混合model的上边界
    println(s"The upper bound on perplexity:$lp")
    
    //Describe topics
    val topics = model.describeTopics(3)
    println("The topics described by their top-weighted terms:")
    topics.show(false)
    
    //show the result
    val transformed = model.transform(dataset)
    transformed.show(false)
*/
  }
}