package study.exception

import scala.Left
import scala.Right
import scala.io.Source
import scala.util.Failure
import scala.util.Success
import scala.util.Try


/**
 * spark异常处理方式,实现返回值的统一，采用monad方式，monad简单说就是为简单的类型提供额外的属性泛型的容器
 * scala提供了至少三种不同的monad类型，分别如下：
 * 
 * 		1.Option和他的两个子类，Some[T]和None.这个monad提供了一个列表，	当我们对错误的详细情况不感兴趣的时候，尅使用它们；
 * 				解决了空指针null的问题
 * 		2.Either和他的两个子类，Left[T]和Right[T].这个monad可以返回两个不同类型的对象，T和K，分别代表正常行为和异常行为；
 * 				解决了返回值不确定的问题（返回两个值中的其中一个）
 *		3.Try和他的两个子类，Success[T]Failure[T].它和Either很像。使用泛型T来替代Left子类。Failure总是Throwable的一个子类（Try在Scala2.10中引入）
 * 				解决可能会抛出异常的问题
 */
object Exception_handle {
  def main(args: Array[String]): Unit = {
    try_demo1
    try_demo2
    either_demo
  }
  
  /*
   * 程序设计中，常常会有这样的要求，一个函数(或方法)在传入不同参数时会返回不同的值。
   * 返回值是两个不同的类型，分别是Left和Right。惯例中我们一般认为Left包含错误或者
   * 无效值，Right包含正确或者有效值。
   * 
   * 在Scala 2.10之前，Either/Right/Left类和Try/Success/Failure类是相似的效果。
   * 
   * 除了使用match case方式来获取数据，我们还可以分别使用 .right.get 和 .left.get 
   * 方法，当然你需要使用 .isRight 或 .isLeft 先判断一下。Left或Right类型也有 filter, 
   * flatMap, foreach, get, getOrElse, map 方法，它们还有toOption, toSeq 方法，
   * 分别返回一个Option或Seq 。
   */
  def either_demo(){
    //定义一个匿名函数
    val divideBy = (x: Int, y: Int) => if(y ==0) Left("Dude, can`t divide by 0")
    else Right(x/y)
    
    divideBy(1, 0) match {
      case Left(l) => println("Answer: "+l)
      case Right(r) => println("Answer: "+r)
    }
  }
  
  
  //使用Try来处理异常
  def try_demo1(){
    //定义一个匿名函数
    val divideBy = (x: Int, y: Int) => Try(x/y)
    
    //测试
    println(divideBy(1,1).getOrElse(0))//1
    println(divideBy(1,0).getOrElse(0))//0
    divideBy(1,1).foreach(println(_))//1
    divideBy(1,0).foreach(println(_))//no print
    
    divideBy(1,0) match{
      case Success(i) => println(s"Success, value is: $i")
      case Failure(s) => println(s"Failured, message is: $s")
    }  
  }
  
  //读取文件内容，如果该方法成功，那么将打印/etc/passwd文件的内容;如果出现异常，那么打印错误信息：
  //java.io.FileNotFoundException: Foo.bar(No such file or directory)
  def try_demo2(){
   //定义一个匿名函数
   val readTextFile = (path: String) => Try(Source.fromFile(path).getLines().toList)
   
   val path = "/etc/passwd"
   readTextFile(path) match {
     case Success(lines) => lines.foreach(println)
     case Failure(f) => println(f)
   }
  }
}




