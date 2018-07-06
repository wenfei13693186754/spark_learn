package com.wdcloud.Utils

import org.apache.spark.mllib.linalg.{Vector, Vectors}

object euclidean extends App{
  def eucl(x: Array[Double], y: Array[Double]):Double = {
    val euc = math.sqrt(x.zip(y).map(p => p._1 - p._2).map(d => d*d).sum)
    1/(1+euc)
  }
  
    val arr1 = Array(1.0,1.0)
    val arr2 = Array(0.0,0.0)
    val arr3 = Array(-3.0,0.5)
    val res0 = eucl(arr1,arr2)
    val res1 = eucl(arr3,arr2)
    println(res0+"///////"+res1)
}