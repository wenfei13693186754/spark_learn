package com.wdcloud.Utils

import model.graphx.operate.TimeOperate
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 测试三种时间衰减算法对时间变化的敏感度
 * 也就是不同的时间差下三种算法算出来的亲密度值的舒密程度
 * 时间差的指数作为分母
  *totalScore = 1/math.pow(x._5/3600000,1.5)
  *时间差的对数作为分母
  *totalScore = 1/math.log(x._5/3600000)
  *使用牛顿冷却定律,t代表的是小时数
  *val t = x._5/3600000
  *totalScore = 1*1/math.pow(math.E, 0.009*t)
 *E:\spark\Spark-GraphX\data\rankAlgorithmTestData\data.txt
 */
object TimeAlgorithmTest {
	val conf = new SparkConf().setAppName("test").setMaster("local[*]")
	val sc = new SparkContext(conf)
	val data = sc.textFile("E:\\spark\\Spark-GraphX\\data\\rankAlgorithmTestData\\data1.txt").filter { x => x!="" && !x.equals(" ") }.map { x => x.split("\\|") }
	
	//调用工具类
	val timeUtil = new TimeOperate()
  
	def main(args: Array[String]): Unit = {
    //NLC()
    //hankerNews()
    myAlgorithm()
  }
 
  
  /*
   * 验证牛顿冷却定律
   * 明显，因为t的值变化范围可以是从秒到月，变化范围很大，导致e的09009t次方的变化范围也特别大
   * 一旦t的值比较大的时候，result就会趋向于0
   */
  def NLC () {
    val data1 = data.map { x => 
      //调用工具类，计算给定时间与当前时间的时间差,并转化为小时
      val t = timeUtil.DealTime(x(1))/(3600000.0*24)
      val result = 1/math.pow(math.E, 0.009*t)
      println(x(0)+"与当前时间差是："+t+"天")
      (x(0),result)
    }
    data1.foreach { x => println(x._1+"  "+x._2) }
    
  }
  
  /*
   * 验证HackerNews算法
   */
  def hankerNews () {
    val data1 = data.map { x => 
      //调用工具类，计算给定时间与当前时间的时间差,并转化为秒
      val t = timeUtil.DealTime(x(1))/1000.0
      println(x(0)+"与当前时间差是："+t+"秒")
      val result = 1/math.pow(t,1.9)
      (x(0),result)
    }.collect()
    data1.foreach { x => println(x._1+"  "+x._2) }
  }
  
  /*
   * 验证分母是对数的排名算法
   */
  def myAlgorithm () {
    val data1 = data.map { x => 
      //调用工具类，计算给定时间与当前时间的时间差,并转化为秒
      val t = timeUtil.DealTime(x(1))/1000.0
      println(x(0)+"与当前时间差是："+t+"秒")
      val result = 1/math.log(t)//loge
      (x(0),result)
    }.collect()
    data1.foreach { x => println(x._1+"  "+x._2) }
  }
}









