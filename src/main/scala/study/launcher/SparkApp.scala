package study.launcher

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkApp extends App{
  val conf = new SparkConf().setAppName("launcherTest").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val rdd = sc.parallelize(Array(1,2,3,4))
  rdd.saveAsTextFile("/launcher_test.dat")
  sc.stop()
}