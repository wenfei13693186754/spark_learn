package com.wdcloud.Utils

import com.google.common.hash.Hashing
import org.apache.spark._

/**
 * 因为对字符串直接取hashCode值可能会有重复的情况，所以这里使用谷歌开发的Guava库的Hashing工具
 * 利用该工具可以通过MD5哈希算法为每个字符串生成一个64位的唯一标示符
 */
object HashingOfGuava {
  val path = "E:\\spark\\Spark-GraphX\\data\\hashingOfGuavaTest\\hashingData"
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("demo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data1 = sc.textFile(path).map { x => hashId("person"+x) }.collect().toList
    val data2 = sc.textFile(path).map { x => hashId("item"+x) }.collect().toList
    val intersect = data1.intersect(data2)
    println(intersect.length)
    
    //val data3 = sc.textFile(path).map { x => hashId(x) }.collect()
    //val proba = data3.distinct.length/data3.length.toDouble
    //println(proba)
  }
  
  //Hashing方法
  def hashId(str: String) = {
    Hashing.md5().hashString(str).asLong()
  }
}