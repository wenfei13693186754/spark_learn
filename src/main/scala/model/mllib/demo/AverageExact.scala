package com.wdcloud.MLlib.demo

/**
 * Function to compute average precision given a set of actual and predicted ratings 
 * 计算推荐的商品的平均准确度
 * actual:给该用户推荐的好友
 * predicted:给该用户推荐的好友
 * 
 * zipWithIndex:
 * returns :   A new iterable collection containing pairs consisting of all elements
                     of this iterable collection paired with their index. Indices start at 0.
  Definition Classes :IterableLike → GenIterableLike
  
  Example:
  
  List("a", "b", "c").zipWithIndex = List(("a", 0), ("b", 1), ("c", 2))
 */
class AverageExact {
  def averageExact(actual: Seq[Int],predicted:Seq[Int],k:Int):Double = {
    val predK = predicted.take(k)
    var score = 0.0
    var numHits = 0.0
    for ((p, i) <- predK.zipWithIndex) {
      if (actual.contains(p)) {
        numHits += 1.0
        
        score += numHits / (i.toDouble + 1.0)
      }
    }
    if (actual.isEmpty) {
      1.0
    } else {
      score / scala.math.min(actual.size, k).toDouble
    }
  }
}