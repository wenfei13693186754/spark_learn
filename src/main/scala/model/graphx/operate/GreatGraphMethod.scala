package study.graphx.operate

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Edge
import breeze.linalg.SparseVector
import scala.io.Source
import org.apache.spark.rdd.RDD
import akka.routing.MurmurHash
import org.apache.spark.graphx.VertexId

object GreatGraphMethod {
  
	val conf = new SparkConf().setAppName("ConnectedComponennts").setMaster("local[*]")
	val sc = new SparkContext(conf)
  val edgePath = "E:\\spark\\Spark-GraphX\\data\\otherData\\other\\followers.txt"
  
  def main(args: Array[String]): Unit = {
    //edgeListFileDemo(sc, edgePath)
    fromEdgeTuplesDemo(sc)
  }
	
	/**
   * Loads a graph from an edge list formatted file where each line contains two integers: a source
   * id and a target id. Skips lines that begin with `#`.
   *
   * If desired the edges can be automatically oriented in the positive
   * direction (source Id < target Id) by setting `canonicalOrientation` to
   * true.
   *
   * @example Loads a file in the following format:
   * {{{
   * # Comment Line
   * # Source Id <\t> Target Id
   * 1   -5
   * 1    2
   * 2    7
   * 1    8
   * }}}
   *
   * @param sc SparkContext
   * @param path the path to the file (e.g., /home/data/file or hdfs://file)
   * @param canonicalOrientation whether to orient edges in the positive
   *        direction
   * @param numEdgePartitions the number of partitions for the edge RDD
   * Setting this value to -1 will use the default parallelism.
   * @param edgeStorageLevel the desired storage level for the edge partitions
   * @param vertexStorageLevel the desired storage level for the vertex partitions
   */
	def edgeListFileDemo( 
	    sc: SparkContext,
      path: String,
      canonicalOrientation: Boolean = false,
      numEdgePartitions: Int = -1,
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[Int, Int] = {
	  
	    GraphLoader.edgeListFile(sc, edgePath, canonicalOrientation, numEdgePartitions, edgeStorageLevel, vertexStorageLevel)
	}
	
	/**
	 * 利用Object Graph工厂方法创建
	 * 此方法通过传入顶点：RDD[(VertexId, VD)]和边：RDD[Edge[ED]]就可以创建一个图
	 * 参数defaultVertexAttr是用来设置那些边中的顶点不在传入的顶点集合当中的顶点的默认属性，所以这个值的类型必须是和传入的顶点的属性的类型一样
	 * 
	 * 源码：
	 * def apply[VD, ED](
      vertices: RDD[(VertexId, VD)],
      edges: RDD[Edge[ED]],
      defaultVertexAttr: VD = null
      ): Graph[VD, ED]
	 */
	def factoryFunctionDemo(sc: SparkContext, namespace: String, edgeTable: String, verTable: String){
	  
	  //val graph = Graph(edges, vertices)
	}
	
	/**
	 * 这个方法可以理解为edgeListFile方法内部就是调用的这个方法，原理就是只根据边：RDD[Edge[ED]]来生成图，顶点就是由所构成边的顶点组成，顶点的默认属性用户可以指定。
	 * 源码：
	 * def fromEdges[VD: ClassTag, ED: ClassTag](
        edges: RDD[Edge[ED]],
        defaultValue: VD): Graph[VD, ED]{}
	 */
	def fromEdges(){
    //通过 .feat 文件读取每个顶点的属性向量
    val featureMap = Source.fromFile("data.feat").getLines().
    map {
      line =>
        val row = line.split(" ")
        //注意：ID 不能之间当作 Long 型的时候 常常用 hashcode 代替
        val key = Math.abs(row.head.hashCode.toLong)
        val feat = SparseVector(row.tail.map(_.toInt))
        (key, feat)
    }.toMap
    //通过 .edges 文件得到两个用户之间的关系 并且计算他们相同特征的个数
    val edges = sc.textFile("data.edges").map {
      line =>
        val row = line.split(" ")
        val srcId = Math.abs(row(0).hashCode.toLong)
        val dstId = Math.abs(row(1).hashCode.toLong)
        val srcFeat = featureMap(srcId)
        val dstFeat = featureMap(dstId)
        val numCommonFeats: Int = srcFeat dot dstFeat
        Edge(srcId, dstId, numCommonFeats)
    }
	  //利用 fromEdges 建立图
    val egoNetwork = Graph.fromEdges(edges, 1)
	}
	
	/**
	 * 这个方法可以理解为edgeListFile方法内部就是调用的这个方法，原理就是只根据边：RDD[(VertexId, VertexId)]来生成图，连边的属性都不知道，默认边的属性当然可以设置，
	 * 顶点就是由所有构成边的顶点组成的，顶点的默认属性用户可以指定，定义如下：
	 * def fromEdgeTuples[VD](
      rawEdges: RDD[(VertexId, VertexId)],
      defaultValue: VD,
      uniqueEdges: Option[PartitionStrategy] = None)
      : Graph[VD, Int]
	 */
	def fromEdgeTuplesDemo(sc: SparkContext){
    // read your file
    /*suppose your data is like 
    v1 v3
    v2 v1
    v3 v4
    v4 v2
    v5 v3
    */
    val file = sc.textFile("E:\\spark\\Spark-GraphX\\data\\createGraphByFromEdgeTuples\\textFile.csv");

    /*
     *  create edge RDD of type RDD[(VertexId, VertexId)]
     *  
     *  MurmurHash.stringHash is used because file contains vertex in form of String . If its of Numeric type then it wont be required .
     */
    val edgesRDD: RDD[(VertexId, VertexId)] = file.map(line => line.split(" "))
      .map(line =>
        (MurmurHash.stringHash(line(0).toString), MurmurHash.stringHash(line(1).toString)))

    // create a graph 
    val graph = Graph.fromEdgeTuples(edgesRDD, 1)

    // you can see your graph 
    graph.triplets.collect.foreach(println)	 
    println("fromEdgeTuples图创建成功")
	}
	
}



















