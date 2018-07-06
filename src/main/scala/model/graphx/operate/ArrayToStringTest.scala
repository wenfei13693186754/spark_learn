package model.graphx.operate

import org.apache.spark._

object ArrayToStringTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("demo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val users = Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                   (5L, ("franklin", "prof")), (2L, ("istoica", "prof")))
    println(users.size)
    val str = users.mkString(" ")
    val arr = str.split(" ")
    println(arr.size)
    
    val st: String = 123456+""
    val aa = 1212121
    println(st.toInt)
    println(aa.toString())
    
    //数组的扩容，动态添加元素********************************************************************************
    val arr1 = Array("a","b","c","d","e")
    val arr2 = Array("f","g","h","i","j")
    val str1 = "holle"
    val arr3 = arr1.++:(arr2)
    val arr4 = arr3.+:(str1)
    println(arr3.mkString(","))
    println(arr4.mkString(","))
  }
}




