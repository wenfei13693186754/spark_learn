package study.accumulator

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.HashSet

/**
 * 定义了一个map类型的累加器
 */
case class Employee(id: String, name: String, dept: String)
object AccumulableCollectionTest {

  private val conf = new SparkConf().setAppName("graphDemo").setMaster("local[*]")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  @transient private val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    setAccuTest
    setValueTest
  }

  def setAccuTest() {
    val setAccu = sc.accumulableCollection(scala.collection.mutable.HashSet[String]())
    val data = sc.parallelize(List("a", "b", "c", "d", "a", "c", "e"))
    data.foreach { x => setAccu.+=(x) }
    val data1 = List("a", "b")
    setAccu.value.--=(data1)//对累加器中的值进行减操作必须在driver端
    val set = setAccu.value
    println(set.mkString(","))
  }

  def setValueTest() {
    val setAccu = sc.accumulableCollection(scala.collection.mutable.HashSet[String]())
    val data = sc.parallelize(List("a", "b", "c", "d", "a", "c", "e"))
    data.foreach { x => setAccu.+=(x) }
    val data1 = new HashSet[String]()
    data1.+=("asdf")
    setAccu.setValue(data1)
    val set = setAccu.value
    println(set.mkString(",")) //asdf
  }
}




