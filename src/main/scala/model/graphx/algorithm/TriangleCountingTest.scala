package study.graphx.algorithm

import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 三角形算法
 * 一个顶点有两个相邻的顶点以及相邻顶点之间的边时，这个顶点是一个三角形的一部分
 * GraphX的这个三角形算法，计算通过每个顶点的三角形数量。
 * 需要注意的是，在计算社交网络数据集的三角形计数时，TriangleCount需要边是规范的方向(srcId<dstId)
 * 并且通过Graph.partitionBy分片过
 * 
 * 适用场景：例如微博上你关注的人也相互关注，大家关注关系中就会有很多三角形，这说明社区很强很稳定，大家的联系比较紧密；
 * 如果说只有你一个人关注很多人，这说明你的社交群体是非常小的。
 */
object TriangleCountingTest {
  def main(args: Array[String]): Unit = {  
    val conf = new SparkConf().setAppName("triCounts").setMaster("local[3]")
		val sc = new SparkContext(conf)
    val edgePath = "E:\\资料\\spark\\Spark-GraphX\\data\\otherData\\other\\followers.txt"
    val userPath = "E:\\资料\\spark\\Spark-GraphX\\data\\otherData\\other\\users.txt"	
	  /*
	   * 读取边列表文件，产生graph
	   * triangleCount算法需要边的方向是规范的方向（srcId<dstId）,并且Graph.partitonBy分片过
	   * srcId和dstId分别对应源和目标顶点的标示符
	   */
	  val graph = GraphLoader.edgeListFile(sc, edgePath, true).partitionBy(PartitionStrategy.RandomVertexCut)
	  
	  
	  //使用graph调用三角算法函数，并返回顶点属性
	  val triCounts = graph.triangleCount().vertices
	  val aa = graph.triangleCount().edges
	  println(aa.collect().mkString("\n"))
	  //读取文件，获取对应的RDD，并执行map操作，返回一个新的RDD
	  val users = sc.textFile(userPath).map{line=>
	  val fields = line.split(",")
	  (fields(0).toLong,fields(1))      
	  }
	  
	  //将triCounts和用户列表对应起来,按照id进行连接
	  val triCountByUsername = users.join(triCounts).map{
	  case(id,(username,tc))=>(id,tc)
			  
	  }
	  println(triCountByUsername.collect().mkString("\n"))    
	  
	  /*
	   *follower.txt     users.txt
	   * 	2 1    				1,BarackObama,American
        4 1						2,ladygaga,American    		
        1 2						3,John,American        		
        6 3						4,xiaoming,Beijing     		
        7 3						6,Hanmeimei,Beijing    		
        7 6						7,Polly,American       		
        6 7						8,Tom,American         		
        3 7
        
        输出结果：可以看到有的是1，有的是0，是1的刚好是经过该顶点的三角形有1个，为0的说明经过该顶点的三角形是0个。
	   * 	(xiaoming,0)
        (Hanmeimei,1)
        (ladygaga,0)
        (BarackObama,0)
        (John,1)
        (Polly,1)	  
	   */
  }
  
}




