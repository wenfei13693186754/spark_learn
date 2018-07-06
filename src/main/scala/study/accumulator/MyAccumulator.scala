package study.accumulator

import scala.collection.immutable.Vector

import org.apache.spark.AccumulatorParam
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 自定义累加器
 * 1.实现AccumulatorParam接口
 * 2.重写zero和addInPlace方法
 */
object MyAccumulator extends AccumulatorParam[Vector[String]] {

  def zero(initialValue: Vector[String]): Vector[String] = {
    initialValue
  }

  def addInPlace(v1: Vector[String], v2: Vector[String]): Vector[String] = {
    v1.++(v2)
  }
}

object MyAccumulatorTest {
  val conf = new SparkConf().setAppName("graphDemo").setMaster("local[*]")
  val sc: SparkContext = new SparkContext(conf)
  def main(args: Array[String]): Unit = {
    val acc = sc.accumulator[Vector[String]](Vector())(MyAccumulator)

    val data = sc.parallelize(List[String]("b","c","d","e"))
    data.foreach { x =>
      val v = Vector(x)
      acc.add(v)
    }
    println(Thread.currentThread().getName + "累加器中内容是：" + acc.value) //main累加器中内容是：Vector(e, b, d, c)
    acc.setValue(Vector("hello word"))
    println(Thread.currentThread().getName + "累加器中内容是：" + acc.value)//main累加器中内容是：Vector(hello word)
  }
}
