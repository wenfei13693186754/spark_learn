package study.function

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext

/**
 * 测试map函数以及其衍生函数
 */
object MapTest {
  val conf = new SparkConf()
  conf.setAppName("test").setMaster("local[*]")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    //mapTest
    //mapPartitionsTest
    //mapPartitionsWithContext
    //mamapPartitionsWithIndexTest
    mapWithTest
  }

  def mapTest() {
    val a = sc.parallelize(1 to 9, 3)
    val b = a.map { x => x * 2 }
    println(b.collect().mkString(",")) //2,4,6,8,10,12,14,16,18
  }

  /*
   * mapPartitions的输入函数应用于每个分区，也就是把每个分区中的内容作为一个整体来处理。
   * def mapPartitions[U: ClassTag](f: Iterator[T] => Iterator[U], preservesPartitioning: Boolean = false): RDD[U]
	 * f即为输入函数，它处理每个分区里面的内容。每个分区中的内容将以Iterator[T]传递给输入函数f，f的输出结果是Iterator[U]。最终的RDD由所有分区经过输入函数处理后的结果合并起来的。
   * 
   * 下边的例子中的函数myfun把分区中的一个元素和它的下一个元素组成一个tuple，因为分区最后一个元素没有下一个元素了，所以(3, 4)和(6, 7)不在结果中
   */
  def mapPartitionsTest() {
    val a = sc.parallelize(1 to 9, 3)
    def myfun[T](iter: Iterator[T]): Iterator[(T, T)] = {

      var res = List[(T, T)]()
      var pre = iter.next()
      while (iter.hasNext) {
        val cur = iter.next
        res = res.::(pre, cur)
        pre = cur
      }
      res.iterator
    }

    val b = a.mapPartitions(myfun).collect()
    println(b.mkString(",")) //(2,3),(1,2),(5,6),(4,5),(8,9),(7,8)
  }

  /*
   * 通过对这个RDD的每个分区应用一个函数来返回一个新的RDD。 这是一个mapPartitions的变体，它也将TaskContext传递给了闭包
   * 与mapPartitions类似，但允许访问mapper内的处理状态信息
   * 
   * TaskContext
   * Contextual information about a task which can be read or mutated during execution. To access the TaskContext for a running task, use:
   * 		org.apache.spark.TaskContext.get()
   * 因为传入了一个TaskContext参数，所以可以获得当前task的一些信息，比如task的partitionId、AttemptID,
   * 以及任务的执行状态，是成功了、运行中还是失败了等，还可以添加一个监听器来监听任务的执行，应用中，如Hadoop中register a callback to close the input stream. 
   */
  def mapPartitionsWithContext() {

    val a = sc.parallelize(1 to 9, 3)  
    def myfunc(tc: TaskContext, iter: Iterator[Int]): Iterator[Int] = {
      tc.addOnCompleteCallback(() => println(
        "Partition: " + tc.partitionId +
          ", AttemptID: " + tc.attemptId))
      /*
       * 
       * addOnCompleteCallback
       * 添加在任务完成时执行的回调函数。 一个例子是HadoopRDD注册一个回调来关闭输入流。 将在任何情况下被呼叫 - 成功，失败或取消。
       * 
       * 这里的输出结果是：
       * Partition: 1, AttemptID: 1
         Partition: 2, AttemptID: 2
         Partition: 0, AttemptID: 0    
       */
          
//      tc.isCompleted()
//      tc.attemptId()
//      tc.runningLocally()
//      tc.addTaskCompletionListener(listener)
//      tc.isInterrupted()
      iter.toList.filter(_ % 2 == 0).iterator
    }
    val b = a.mapPartitionsWithContext(myfunc).collect
    println("@"+b.mkString(","))//@2,4,6,8
  }
  
  /**
   * Similar to mapPartitions, but takes two parameters. 
   * The first parameter is the index of the partition and
   *  the second is an iterator through all the items within 
   *  this partition. The output is an iterator containing the 
   *  list of items after applying whatever transformation the function encodes.
   * 
   * def mapPartitionsWithIndex[U: ClassTag](f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean = false): RDD[U]
   */
  def mamapPartitionsWithIndexTest(){
    val a = sc.parallelize(1 to 9, 3)
    
    def myfun(index: Int, iter: Iterator[Int]): Iterator[String] = {
      iter.map { x => index+","+x }
    }
    
    val b = a.mapPartitionsWithIndex(myfun).collect()
    println(b.mkString(","))//0,1,0,2,0,3,1,4,1,5,1,6,2,7,2,8,2,9
  }  
  
  /**
   * def mapWith[A: ClassTag, U: ](constructA: Int => A, preservesPartitioning: Boolean = false)(f: (T, A) => U): RDD[U]
   *	   第一个函数constructA是把RDD的partition index（index从0开始）作为输入，输出为新类型A；
   * 	   第二个函数f是把二元组(T, A)作为输入（其中T为原RDD中的元素，A为第一个函数的输出），输出类型为U。
   *
   */
  def mapWithTest(){
    val a = sc.parallelize(1 to 9, 3)
    val b = a.mapWith(a => a * 10)((x, y) => x +y).collect()
    println(b.mkString(","))//1,2,3,14,15,16,27,28,29
  }
}















