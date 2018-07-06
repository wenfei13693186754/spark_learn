package study.accumulator

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 基本数据类型累加器操作
 */
object NumicAccu {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(conf)
    val numicAccu = sc.accumulator(0, "NumicAccu")
    //读取文件中数据，每有一条数据，累加器增1，最终累加器中的值就是文件中 的数据的条数
    sc.textFile("").map { x => numicAccu.add(1) }
    //不管累加器中之前多少，现在将其值设置为1
    numicAccu.setValue(1)
    
    println(numicAccu.zero)
  }
}