package study.util

import java.util.concurrent.ConcurrentHashMap

object ConcurrentHashMapDemo {
  def main(args: Array[String]): Unit = {
    val chm = new ConcurrentHashMap[String, String]()
    chm.put("", "")
    chm.get("")
    //......
  }
}