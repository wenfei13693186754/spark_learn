package com.wdcloud.Utils

import org.apache.spark._
import scala.collection.immutable.Set
import java.io.PrintWriter
import java.io.File

/**
 * 验证不同业务id下的hash一致性，也就是比如100万的people_1（1--100万），经过hashCode后放到set集合中，
 * 看是否会有重合的。另外再有item_1（1--100万），经过hashCode后看两个集合会不会有重合的情况出现。
 */
object HashCodeTest {
	val conf = new SparkConf().setMaster("local[*]").setAppName("HashCodeTest")
	val sc = new SparkContext(conf)
	val path1 = "E:\\spark\\Spark-GraphX\\data\\hashCodeTest\\hashCodeTest_item.txt"
	val path2 = "E:\\spark\\Spark-GraphX\\data\\hashCodeTest\\hashCodeTest_person.txt"
  def main(args: Array[String]): Unit = {
   //createData("person",5000000,path2)
   //repetitionRate()
   repetitionRateBetweenFile(path1, path2)
   
  }
  
	/*
	 * 用来测试给定数据集中的数据经过hash算法后的重复率
	 */
  def repetitionRate () {
	  val data = sc.textFile(path2)
	  val hashData = data.map { x => Math.abs(x.hashCode.toLong) }.collect()
	  val count = hashData.size
	  //将Array转化为set集合
	  val setData = hashData.toSet
	  val setCount = setData.size
	  val percent = (count - setCount)/count.toDouble
	  println("重复率是："+percent)
  }
  
  /*
   * 用来测试person_id和item_id类型数据各100万，经过hashCode算法后，二者中会不会有重复的
   */
  def repetitionRateBetweenFile (path1: String, path2: String) {
    val data1 = sc.textFile(path1)
    val data2 = sc.textFile(path2)
    val hashData1 = data1.map { x => Math.abs("person".hashCode())+""+Math.abs(x.hashCode())}.collect().toSet
    val hashData2 = data2.map { x => Math.abs("item".hashCode())+""+Math.abs(x.hashCode())}.collect().toSet
    
    //连接两个集合，如果集合中有重复的元素，那么会自动过滤掉
    val result = hashData1.++(hashData2)
    
    //开始计算重复率
    val count1 = hashData1.size + hashData2.size
    val count2 = result.size
    
    val percent = (count1 - count2)/count1.toDouble
    println("两个set集合共有的数据量："+count1+"  合并后的集合的数据量："+count2+"  重复率："+percent)
  }  
  
  /*
   * 生成数据
   */
  def createData (str: String, data: Int,path: String) {
    val writer = new PrintWriter(new File(path))    
    for (line <- 0 to data) {
      writer.println(str+"_"+line)
    }
    writer.close()
  }
}









