package model.graphx.operate

import org.apache.spark.graphx.{EdgeTriplet, VertexId, Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


//定义下面的类将 ingredient(原料) 和 compount(化合物) 统一表示 注意父类一定要可以序列化
class FoodNode(val name: String) extends Serializable
case class Ingredient(override val name: String, val cat: String) extends FoodNode(name)
case class Compound(override val name: String, val cas: String) extends FoodNode(name)
/**
  * 创建二分图.
  */
object bipartiteGraph {
  val projectDir = "E:\\spark\\Spark-GraphX\\data\\ingr_comp"
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkInAction").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //读取原料文件信息
    val ingredients: RDD[(VertexId, FoodNode)] = sc.textFile(projectDir + "\\ingr_info.tsv").filter {
      !_.startsWith("#")//!-->用来否定一个布尔表达式
    }.map {
      line =>
        val array = line.split("\t")
        (array(0).toLong, Ingredient(array(1), array(2)))
    }
    //获取得到最大的 ingredient 的ID 并且加1
    val maxIngrId = ingredients.keys.max() + 1
    //读取化合物基础信息
    val compounds: RDD[(VertexId, FoodNode)] = sc.textFile(projectDir + "\\comp_info.tsv").filter {
      !_.startsWith("#")
    }.map {
      line =>
        val array = line.split("\t")
        (maxIngrId + array(0).toLong, Compound(array(1), array(2)))
    }
    //根据文件 ingr_comp.csv 生成边，注意第二列的所有顶点都要加上 maxIngrId
    val links = sc.textFile(projectDir + "\\ingr_comp.tsv").filter {
      !_.startsWith("#")
    }.map {
      line =>
        val array = line.split("\t")
        Edge(array(0).toLong, maxIngrId + array(1).toLong, 1)
    }
    //将两个顶点合并
    val vertices = ingredients ++ compounds
    val foodNetWork = Graph(vertices, links)
    //foodNetWork.vertices.take(10).foreach(println)
    //访问一下这个网络前面5条triplet的对应关系
    foodNetWork.triplets.take(5).foreach(showTriplet _ andThen println _)
  }

  def showTriplet(t: EdgeTriplet[FoodNode, Int]): String =
    "The ingredient " ++ t.srcAttr.name ++ " contains " ++ t.dstAttr.name 
}