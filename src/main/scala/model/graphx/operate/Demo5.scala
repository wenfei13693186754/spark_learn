package com.wdcloud.graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.impl.GraphImpl

/**
 * 聚合操作
 * 收集follower的年龄，并将年龄求和
 */
object Demo5 {
	val conf = new SparkConf().setAppName("graph").setMaster("local[*]")
  val sc = new SparkContext(conf)
  def main(args: Array[String]): Unit = {
    //Create an RDD for the vertices
    val users: RDD[(VertexId,(String,Int))] = sc.parallelize(Array((3L, ("rxin", 20)), (7L, ("jgonzal",34)),
                       (5L, ("franklin", 32)), (2L, ("istoica", 36))))
    //Create an RDD for Edges
    val relationships: RDD[Edge[String]] = sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                       Edge(2L, 5L, "colleague"), Edge(5L, 7L, "colleague")))
    //Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe",25)
    //Build the initial Graph
    val graph = Graph(users,relationships,defaultUser)
    
    //开始聚合消息
    /*
     * 1. 首先调用这个方法的时候，因为aggregateMessages返回的是VertexRDD[Msg],msg类型是用户指定的，
     *    所以这里需要指定出来，不然编译器无法通过隐式反馈反馈出返回值类型从而报错。可以看到这里有两种
     *    定义方式。
     * 2.sendMsg: 方法里边的参数triplet是EdgeContext类型的，用它来定义sendMsg方法。
     * 		EdgeContext对象中有srcId,dstId,srcAttr,dstAttr,attr，另外还有sendToSrc和sendToDst方法，
     *    用来发送消息，并且这里就直接指明了发送消息的方向。
     * 		注意sendToSrc和sendToDst方法中的参数的类型就是VertexRDD中的参数类型。
     * 3.merge函数：用来对一个顶点上收到的两个消息进行合并为一个消息的处理，
     * 		对于没有收到消息的顶点不做处理
     * 4.TripletFields：是一个可选的属性，用来指明那些数据可以被EdgeContext访问
     * 		默认是TripletFields.ALL,说明用户定义的sendMsg函数可以访问EdgeContext中的任何信息。
     * 		例如：
     * 				如果我们想计算每个用户的follower的平均年龄，那么我们只需要source fields,这时我们将使用
     * 				TripletFields.Src来限定我们只需要source fields
     * 5.返回值：VertexRDD[msg],其中msg的类型是用户定义好的，就是图上每个顶点的数据类型。
     */
    //val nbrs:VertexRDD[(String, Int)] = graph.aggregateMessages(
    val nbrs = graph.aggregateMessages[(String, Int)](
        triplet => 
          triplet.sendToDst(triplet.srcAttr),
          (x, y) => (x._1,x._2+y._2),   
          //TripletFields.Dst//报空指针异常，因为我们只是使用sendToDst方法向dst发送了消息，没有使用sendToSrc方法，所以这里限定每个顶点值接收dst顶点发来的消息，就会报空指针了
          //TripletFields.None//包IOException
          TripletFields.Src
    )
    //nbrs.foreach(x => println(x._1+" "+x._2))
    demo()
  }
	
	def demo(){
	   //Create an RDD for the vertices
    val users: RDD[(VertexId,(String,Int))] = sc.parallelize(Array((1L, ("rxin", 20)), (2L, ("jgonzal",34)),
                       (3L, ("franklin", 32))))
    //Create an RDD for Edges
    val relationships: RDD[Edge[String]] = sc.parallelize(Array(Edge(1L, 2L, "collab"),Edge(3L, 1L, "advisor")))
    //Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe",25)
    //Build the initial Graph
    val graph = Graph(users,relationships,defaultUser)
    
    val nbrs = graph.aggregateMessages[(String, Int)](
        triplet => 
          triplet.sendToDst(triplet.srcAttr),
          (x, y) => (x._1,x._2+y._2),   
          //TripletFields.Dst//报空指针异常，因为我们只是使用sendToDst方法向dst发送了消息，没有使用sendToSrc方法，所以这里限定每个顶点值接收dst顶点发来的消息，就会报空指针了
          //TripletFields.None//包IOException
          TripletFields.All
    )
    nbrs.foreach(x => println(x._1+" "+x._2))
	}
}
















