package study.function

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 *def aggregate[U](zeroValue: U)(seqOp: (U, Int) => U, combOp: (U, U) => U)(implicit evidence$33: ClassTag[U]): U
 * seqOp的操作是遍历各个分区中的所有元素(T),第一个T跟zeroValue做聚合操作，它们的结果再和第二个T做聚合操作，直到遍历完分区中的额所有元素。
 * combOp的操作是把各个分区聚合的结果再聚合。
 * zeroValue是初始值
 * 
 * aggregate函数返回一个跟RDD不同类型的值，因此，需要一个操作seqOp来把分区中的元素T合并成为一个U，另一个操作combOp把所有U进行聚合。
 */
object AggregatTest {
  
  val conf = new SparkConf().setAppName("test").setMaster("local[*]")
  val sc = new SparkContext(conf)
  
  //def aggregate[U](zeroValue: U)(seqOp: (U, Int) => U, combOp: (U, U) => U)(implicit evidence$33: ClassTag[U]): U
  def main(args: Array[String]): Unit = {
    val a = sc.parallelize(1 to 9, 3)
    val b = a.aggregate("#")((x, y) => x + "" + y, (x, y) => x + y)
    println(b)//##789#123#456
  }
}