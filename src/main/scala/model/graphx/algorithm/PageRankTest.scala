package study.graphx.algorithm

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * pageRank算法验证
 * 	pageRank算法是用来衡量每个顶点的重要性的，假设从u到v的边表示u对v的重要性的认可，
 * 		例如，如果Twitter用户被许多其他用户跟随，则该用户的排名将比较高
 * 	Graphx提供了PageRank的静态和动态的实现，静态的pageRank运行固定次数的迭代，而
 * 		动态pageRank运行直到排名收敛。GraphOps运行直接调用这些算法作为Graph上的方法使用
 * 
 * 注意：
 * 	1.pageRank计算排名的时候考虑边的方向吗？
 * 		pageRank算法计算排名时候会考虑边的方向，因为其底层使用的是pregel，pregel指定的方向似乎out，也就是当某个顶点是活跃顶点时候，会向该顶点出度方向发送消息。
 * 	
 *  2.pageRank计算排名时候对于某个顶点出度和入度是如何影响得分的？
 *  	因为底层pregel的消息发送方向是out，也就是每个顶点的出度方向，所以某个顶点的出度边是该顶点的消息发送方向，入度边那么就是别的临边的消息发送方向而已
 *  3.pageRank算法为什么要指定一个tol值来使得计算收敛呢？
 *  	
 *  
 *  	
 * 	边的属性值：
 * 		1/src顶点的出度
 *  顶点的属性
 *  	顶点属性值的初始化，但是属性值带两个参数即（初始PR值，两次迭代结果的差值）  
      .mapVertices( (id,attr) => (0.0, 0.0) )  
 */
object PageRankTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("graphDemo").setMaster("local[*]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc: SparkContext = new SparkContext(conf) 
    
    val edgePath = "E:\\spark\\Spark-GraphX\\data\\otherData\\other\\followers.txt"
    val userPath = "E:\\spark\\Spark-GraphX\\data\\otherData\\other\\users.txt"
    
    //edgeListFile读取边列表文件的时候，要求文件内容必须是vid vid 
	  val graph = GraphLoader.edgeListFile(sc,edgePath)

	  //用上边的图框架调用pagerank（动态）算法，静态调用的方法名是staticPageRank(Int)
	  //传入的这个参数的值越小PageRank计算的值就越精确，如果数据量特别大而传入的参数值又特别小
	  //的情况下就会导致巨大的计算任务和计算时间。 
	  //vertices将返回顶点属性
	  val ranks = graph.pageRank(0.001).vertices
	  ranks.foreach(x =>println(x._1+"****"+x._2))
	  //将上边得到的ranks（顶点属性）和用户进行关系连接
	  // 首先也是读取一个包含了用户信息的文件，然后调用了一个map函数，即将文件里的每行数据
	  //按 ”,” 切开并返回存储了处理后数据的RDD  
	  val users = sc.textFile(userPath).map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
	  
	  //这里具体实现了将ranks和用户列表一一对应起来
	  //从map函数的内容可以看出来按id来进行连接，但是返回的结果只包含用户名和它相应的rank
	  val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }.sortBy(x => x._2, false)
    
	  //打印結果
	  /*
      (BarackObama,0.43383928571428576)
      (ladygaga,0.3063392857142857)
      (John,0.2425892857142857)
      (xiaoming,0.2000892857142857)
      (Polly,0.1682142857142857)
      (Tom,0.1682142857142857)
      (Hanmeimei,0.1682142857142857)
	   */
	  println(ranksByUsername.collect().mkString("\n"))
	  val PR = Array.fill(1)(1.0)
  }
}





