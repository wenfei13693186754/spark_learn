package com.wdcloud.MLlib.recBasedLabel

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 
 */
object recBasedLabel {
  def main(args: Array[String]): Unit = {
    //调用SimlarityRank计算出和用户口味最相投的电影，并返回
    val userProfile = SimilarityRank.userProfile("12")
    //val simMovie = SimilarityRank.simItemRec(userProfile)
    //simMovie.foreach{x => println(x._1+"**"+x._2)}
  }
}  