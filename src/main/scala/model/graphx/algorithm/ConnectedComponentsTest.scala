package model.graphx.algorithm

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 连通图和强连通图验证
 * 应用场景：
 * 	用于进行社区发现
 *	 如在人际关系网中，可以发现出具有不同兴趣、背景的社会团体，方便进行不同的宣传策略；在交易网中，不同的社区代表不同购买力的客户群体，
 * 方便运营为他们推荐合适的商品；在资金网络中，社区有可能是潜在的洗钱团伙、刷钻联盟，方便安全部门进行相应处理；在相似店铺网络中，社区
 * 发现可以检测出商帮、价格联盟等，对商家进行指导等等。总的来看，社区发现在各种具体的网络中都能有重点的应用场景，
 * 
 */
object ConnectedComponentsTest {

	val conf = new SparkConf().setAppName("ConnectedComponennts").setMaster("local[*]")
	val sc = new SparkContext(conf)
  val edgePath = "D:\\Other\\data\\spark\\followers.txt"
  val userPath = "D:\\Other\\data\\spark\\other\\users.txt"
  
  def main(args: Array[String]): Unit = {
    //cc()
    scc()
  }
	
	/**
	 * 强连通图
	 * 	如果有向图G中的每两个顶点都是强连通，那么称其为强连通图。
	 * 强连通
	 * 	在有向图G中，如果两个顶点vi、vj之间(vi-->vj)有一条从vi到vj的有向路径，同时还有一条从vj到vi的有向路径，则称两个顶点强连通
    * 返回的结果，以强连通图中最小的id作为label来标识途中顶点所属的连通图
	 */
	def scc (){
	  
	  // Load the graph as in the PageRank example
	  val graph = GraphLoader.edgeListFile(sc, edgePath)
    // 用上边的图框架调用connectedComponents算法，
	  val scc = graph.stronglyConnectedComponents(5).vertices  
	  graph.vertices.foreach(x => println(x._1+"|&&|"+x._2))
	  scc.foreach(x => println(x._1+"||"+x._2))
	  //将上边的cc(顶点属性)和用户进行关系连接
    //首先读取一个包含了用户信息的文件，然后调用map函数，将文件中每一行数据进行split操作
	  //并返回存储了集合中指定数据的RDD
	  val users = sc.textFile(userPath).map { line =>
  	  val fields = line.split(",")
  	  (fields(0).toLong, fields(1))
	  }    
    
    //这里具体实现了将cc和用户列表一一对应起来
    //从map函数的内容可以看出来按id来进行连接，但是返回的结果只包含用户名和它对应的cc
	  val ccByUsername = users.join(scc).map {
	    case (id, (username, cc)) => (username, cc)
	  }
    // Print the result  
	  println(ccByUsername.collect().mkString("\n"))	
	  /*
	   * follower.txt     users.txt
	   * 	2 1    				1,BarackObama,American
        4 1						2,ladygaga,American    		
        1 2						3,John,American        		
        6 3						4,xiaoming,Beijing     		
        7 3						6,Hanmeimei,Beijing    		
        7 6						7,Polly,American       		
        6 7						8,Tom,American         		
        3 7
           
	   * 输出结果是：
	   *  (xiaoming,4)
        (Hanmeimei,3)
        (ladygaga,1)
        (BarackObama,1)
        (John,3)
        (Polly,3)
        
        问题：1.为什么强联通体的标示用1、3和4来表示呢？
       	 因为强连通图计算的是每个顶点的连接组件的成员资格，并返回每个连接组件，
       	 以及该组件中所包含的每个成员，这些成员的attr就是这个组件成员最小的那个id.
       	 所以这里使用强连通图会返回3中标示符。
       	 2.最低顶点如何定义的？
       	 	所谓最低顶点就是指的哪个顶点的id值最小，这个最小值就是这个连通图的最终标识。      
	   */  
	}
  
	/**
	 * 连通图
	 * 	如果图中任意两点都是连通的，那么此图称为连通图
    * 连通图以图中最小的ID作为label给图中顶点打属性
	 */
  def cc (){

	  // Load the graph as in the PageRank example
	  val graph = GraphLoader.edgeListFile(sc, edgePath)
    // 用上边的图框架调用connectedComponents算法，
	  val cc = graph.connectedComponents()//.vertices  
	  
	  graph.vertices.foreach(x => println(x._1+"|&&|"+x._2))
	  //cc.foreach(x => println(x._1+"||"+x._2))
	  //将上边的cc(顶点属性)和用户进行关系连接
    //首先读取一个包含了用户信息的文件，然后调用map函数，将文件中每一行数据进行split操作
	  //并返回存储了集合中指定数据的RDD
	  val users = sc.textFile(userPath).map { line =>
  	  val fields = line.split(",")
  	  (fields(0).toLong, fields(1))
	  }
    
    //这里具体实现了将cc和用户列表一一对应起来
    //从map函数的内容可以看出来按id来进行连接，但是返回的结果只包含用户名和它对应的cc
	 /* val ccByUsername = users.join(cc).map {
	    case (id, (username, cc)) => (username, cc)
	  }*/
    // Print the result  
	 // println(ccByUsername.collect().mkString("\n"))
	  
	  /*
	   * follower.txt     users.txt
	   * 	2 1    				1,BarackObama,American
        4 1						2,ladygaga,American    		
        1 2						3,John,American        		
        6 3						4,xiaoming,Beijing     		
        7 3						6,Hanmeimei,Beijing    		
        7 6						7,Polly,American       		
        6 7						8,Tom,American         		
        3 7
           
	   * 输出结果是：
	   *  (xiaoming,1)
        (Hanmeimei,3)
        (ladygaga,1)
        (BarackObama,1)
        (John,3)
        (Polly,3)  
        
        问题：1.为什么联通体的标示用1和3来表示呢？
       	 因为连通图计算的是每个顶点的连接组件的成员资格，并返回包含该顶点的连接组件中，每个组件都包含最低顶点id的图。
       	 2.最低顶点如何定义的？
       	 	所谓最低顶点就是指的哪个顶点的id值最小，这个最小值就是这个连通图的最终标识。      
	   */    
}
















