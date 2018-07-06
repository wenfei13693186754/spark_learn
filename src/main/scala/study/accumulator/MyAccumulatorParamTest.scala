package study.accumulator

import scala.collection.mutable.Map
import scala.io.Source

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.AccumulatorParam

/**
 * 自定义累加器
 */
object MyAccumulatorParamTest extends AccumulatorParam[Map[String, Long]] {
  def zero(initialValue: Map[String, Long]): Map[String, Long] = {
    initialValue
  }

  def addInPlace(m1: Map[String, Long], m2: Map[String, Long]): Map[String, Long] = {
    this.synchronized(m1).++=(m2)
  }
}

object Test {
  private val conf = new SparkConf().setAppName("graphDemo").setMaster("local[*]")
  @transient private val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    // Then, create an Accumulator of this type:
    val accMap = sc.accumulator(Map[String, Long]())(MyAccumulatorParamTest)
    accMap.add(Map("a" -> 1L))
    println(accMap.value.mkString(","))
  }
}


