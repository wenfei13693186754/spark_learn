package study

import java.io.File
import java.io.PrintWriter

import scala.collection.mutable.HashMap
import scala.io.Source
import scala.util.matching.Regex

/**
 * 对将向neo4j中写入的数据进行预处理
 * 	1.将","替换为<tab>
 * 	2.将“替换为去除掉
 * 	3.将替换完产生的空行过滤掉
 * 	4.将不规则数据进行整理，比如某些数据不该换行的时候换行了，需要将其重新和前一行合并。
 */
object Neo4jDataHandle {
  def main(args: Array[String]): Unit = {
    val Syspath = "C:\\Users\\Administrator\\Desktop\\寻之途\\"
    val file = "knowledge_nodes2"
    val sourceCsv = Syspath + file
    val targetCsv = Syspath + "neo4j_data\\" + file
    val label = "knowledge"
    nodeDataHandle(label, sourceCsv, targetCsv)
    //edgeDataHandle(sourceCsv, targetCsv)
  }

  //边数据处理
  def edgeDataHandle(sourceCsv: String, targetCsv: String) {
    val writer = new PrintWriter(new File(targetCsv + ".csv"))
    val data = Source.fromFile(sourceCsv + ".csv")
      .getLines()
      .map { x => x.replaceAll("\",\"", "\t").replaceAll("\"", "") }
      .foreach { x =>
        writer.println(x + "\t" + "parent")
      }
    writer.flush()
    writer.close()

  }

  //neo4j顶点数据处理
  def nodeDataHandle(label: String, sourceCsv: String, targetCsv: String) {
    val tmpFile = new File(sourceCsv + "_tmp.csv")
    val w1 = new PrintWriter(tmpFile)
    val source1 = Source.fromFile(sourceCsv + ".csv")
    val lines = source1.getLines().toList
    //获取第一行元素（就是数据中的标题）
    val firstEle = lines(0).split("\",\"")
    //获取数据的字段数
    val size = firstEle.size
    //获取第一行元素中的第一个字段，用来后边放到map中作为key，目的就是，通过key获取到该元素，然后拿出来放到文件第一行，接着删掉这行元素防止重复
    val firstEle_code = firstEle.apply(0).replaceAll("\"", "")
    //对数据进行处理，用<tab>替换掉"\",\"",并去掉数据两边的“\"”,然后去掉空的数据
    val data = lines.map { x =>
      x.replaceAll("\",\"", "\t")
        .replaceAll("\"", "")
    }.filter { x => x.trim().size != 0 }

    //对本来是一行的数据但是分成了多行进行展示的进行处理，拼接成一行
    var str = ""
    val reg = new Regex("^[a-zA-Z0-9]")
    data.foreach { x =>
      if (reg.findFirstIn(x) != None) {
        str = x
      } else {
        str = str + x
      }
      if (str.split("\t").size == size) {
        w1.println(str)
        str = ""
      }
    }

    //将处理完的数据放到临时文件中，关掉数据流
    w1.flush()
    w1.close()
    source1.close()

    //下边读取临时文件中的数据，然后去除掉重复的数据，并写入到指定的文件
    val w2 = new PrintWriter(new File(targetCsv + ".csv"))
    val source2 = Source.fromFile(tmpFile)
    val iter = source2.getLines.map { x =>
      val arr = x.split("\t")
      (arr(0), x)
    }
    
    var map = new HashMap[String, String]()
    while (iter.hasNext) {
      val info = iter.next()
      map.+=(info._1 -> info._2)
    }
    w2.println(label + "\t" + map.apply(firstEle_code))
    map.-=(firstEle_code)
    map.foreach { x =>
      w2.println(label + "\t" + x._2)
    }
    
    w2.flush()
    w2.close()
    source2.close()
    tmpFile.delete()
  }
}