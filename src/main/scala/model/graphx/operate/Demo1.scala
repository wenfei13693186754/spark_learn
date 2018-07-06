package model.graphx.operate

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader
/**
 * 假定想从一些文本文件中构建一个图，限制这个图包含重要的关系和用户，
 * 并且在子图上允许pagerank,最后返回与top用户相关的属性
 */
object Demo1 {
  def main(args: Array[String]): Unit = {
    //1.创建本地sc对象
    val conf = new SparkConf().setAppName("Demo1").setMaster("local[*]")
    val sc = new SparkContext(conf)

    /*
     * Array[Array[String]] = Array(Array(1, BarackObama, American), Array(2, ladygaga, American), 
     * Array(3, John, American), Array(4, xiaoming, Beijing), Array(5, apple, American), Array(6, Hanmeimei, Beijing),
     *  Array(7, Polly, American), Array(8, Tom, American), Array(9, Coll, Englend), Array(10, Banana, Japan))
     */
    val rdd = sc.textFile("E:\\users.txt").map{ lines => lines.split(",")}
    /*
     * Array[(Long, Array[String])] = Array((1,Array(BarackObama, American)), (2,Array(ladygaga, American)), 
     * (3,Array(John, American)), (4,Array(xiaoming, Beijing)), (5,Array(apple, American)), (6,Array(Hanmeimei, Beijing)),
     *  (7,Array(Polly, American)), (8,Array(Tom, American)), (9,Array(Coll, Englend)), (10,Array(Banana, Japan)))
     */
    val users = rdd.map{parts=>(parts.head.toLong,parts.tail)}
    /*
     * 解析edge数据，读取边列表文件
     * Loads a graph from an edge list formatted file where each line contains two integers:a source id and a target id.
     * Skips lines that begin with #
     * If desired the edges can be automatically oriented in the positive direction (source Id < target Id) by setting canonicalOrientation to true.
     */
    val followerGraph = GraphLoader.edgeListFile(sc,"E:\\followers.txt")
    //attach the user attribute
    /*
     * outerJoinVertices方法：join the vertices with the entries in the table RDD and merges the results using mapFunc
     * The input table should contain at most one entry for each vertex. If no entry in other is provided for a particular 
     * vertex in the graph, the map function receives None.
     */
    val graph = followerGraph.outerJoinVertices(users){
        case (uid,deg,Some(attrList))=> attrList
        //一些用户可能没有属性，所以我们设置其为空
        case (uid,deg,None) =>Array.empty[String]
    }
    
    /*
     * 在源graph上过滤出子graph
     * vpred：the vertex predicate, which takes a vertex object and evaluates to true if the vertex is to be included in the subgraph
     * epred：the edge predicate, which takes a triplet and evaluates to true if the edge is to remain in the subgraph. Note that only 
     * 				edges where both vertices satisfy the vertex predicate are considered.
     * return:the subgraph containing only the vertices and edges that satisfy the predicates
     */
    //通过username和name来限制每个用户的graph,这里是username和contryname的和是2的结果
    val subgraph = graph.subgraph(vpred = (vid,attr)=>attr.size==2)
    //计算pageRand
    val pagerankGraph = subgraph.pageRank(0.01)
    //获取到pagerank排名最靠前的users的属性
    val userInfoWithPageRank = subgraph.outerJoinVertices(pagerankGraph.vertices){
      case (uid,attrList,Some(pr))=>(pr,attrList.toList)
      case (uid,attrList,None)=>(0.0,attrList.toList)   
    }
    //_._2._1含义是从前边得到的数据中获取它的第二个元素中的第一个元素用来作为分组的标准
    println(userInfoWithPageRank.vertices.top(5)(Ordering.by(_._2._1)).mkString("\n"))
  }
}