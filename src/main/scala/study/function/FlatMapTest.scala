package study.function

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 测试flatMap函数及其衍生函数
 */
object FlatMapTest {
  
  val conf = new SparkConf().setAppName("test").setMaster("local[*]")
  val sc = new SparkContext(conf)
  
  def main(args: Array[String]): Unit = {
    //flatMapTest
    //flatMapWithTest
    flatMapValuesTest
  }
  
  /*
   * 和map函数相似，但是运行产生对应于每个RDD中的元素产生多个输出
   * def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U]
   * 	可以看到flatMap函数接收的参数是一个函数，函数接收的参数是泛型的，函数的返回值是一个TraversalbeOnce集合的顶级父类。
   * 所以，定义的函数的返回值必须是集合类型的
   */
  def flatMapTest(){
    val a = sc.parallelize(1 to 3, 3)
    val b = a.flatMap { x => 1 to x }.collect()
    println(b.mkString(","))//1,1,2,1,2,3
  }
  
  /*
   * 与flatMap类似，但允许从flatMap函数中访问分区索引或分区索引衍生出来的信息。
   * def flatMapWith[A: ClassTag, U: ClassTag](constructA: Int => A, preservesPartitioning: Boolean = false)(f: (T, A) => Seq[U]): RDD[U]
   * 
   */
  def flatMapWithTest(){
    val a = sc.parallelize(1 to 4, 2)
    val b = a.flatMapWith(x => x, true)((x, y) => List(y, x)).collect()
    println(b.mkString(","))// 0,1,0,2,1,3,1,4
  }
  
  /**
   * flatMap类似于mapValues,不同的地方在于flatMapValues应用于KV对的RDD中的value.每个元素的value被输入函数映射为一个TraversableOnce，然后再与原RDD中的key组成一系列新的KV对
   *
   * 下边例子中原RDD中每个元素的值被转换为一个序列（从其当前值到5），比如第一个KV对(1,2), 其值2被转换为2，3，4，5。然后其再与原KV对中Key组成一系列新的KV对(1,2),(1,3),(1,4),(1,5)。 
   * */
  def flatMapValuesTest(){
    val a = sc.parallelize(List((1, 2), (3, 4), (3, 6)))
    val b = a.flatMapValues { x => x to 5 }.collect()
    println(b.mkString(","))// (1,2),(1,3),(1,4),(1,5),(3,4),(3,5)
  }  
  
}







