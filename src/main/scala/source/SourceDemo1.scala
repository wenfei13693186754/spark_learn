package source


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.Logging

/**
 * 源码跟读
 */
object SourceDemo1 extends Logging{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SourceDemo")
    conf.setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    
    val data = Array(1, 2, 3, 4, 5)  
    val aa = Seq(data).flatten
    println("flatten: "+aa)//flatten: List(1, 2, 3, 4, 5)
    val rdd = sc.parallelize(data)  
    rdd.foreach { x => println(x) }
  }
}