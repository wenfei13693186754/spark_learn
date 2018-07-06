package study.function

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CheckPointTest {
  
  val conf = new SparkConf().setAppName("test").setMaster("local[*]")
  val sc = new SparkContext(conf)
  
  def main(args: Array[String]): Unit = {
    val a = sc.parallelize(1 to 9, 3)
    sc.setCheckpointDir("E:\\checkpointfile\\")
    a.checkpoint()
    a.count()
  }
}