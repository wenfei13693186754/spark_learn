package com.wdcloud.Utils

import org.apache.spark._
import org.apache.spark.graphx._

/**
 * 使用pregel来实现二度好友的推荐
 * 	使用Pregel API来实现这个功能实际上是为了对原来直接使用filter层层过滤来实现二度好友的推荐的优化
 * 	因为使用这个我可以直接指定迭代次数是两次，这样正好迭代到指定用户的二度好友位置，然后使用写好的函数
 *  对边上算好的一度好友之间的亲密度进行权重计算，算出二度好友和指定用户的亲密度。
 *   def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]  
       (graph: Graph[VD, ED],  
       initialMsg: A,  
       maxIterations: Int = Int.MaxValue,  
       activeDirection: EdgeDirection = EdgeDirection.Either)  
       (vprog: (VertexId, VD, A) => VD,  
       sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],  
       mergeMsg: (A, A) => A)  
    : Graph[VD, ED] {}
 */
object RecTwoDegreeFriendsByPregel {
  
  def cohesion (graph: Graph[Array[String], Double]):VertexRDD[Array[String]] = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RecTwoDegreeFriendsByPregel")
    val sc = new SparkContext(conf)
    
    //设定源顶点，也就是目标用户
    val sourceId: VertexId = 443164103
    graph.mapTriplets(x => x)
    
    //首先给图中的顶点的属性中初始化一个值，值为“0.0”，把这个值放在顶点属性数组的第一位
    val initialGraph = graph.mapVertices((id, attr) =>if(id == sourceId) attr.:+("0.0") else attr.:+("100"))
    //开始执行pregel操作
    //先给每个顶点发送一个初始值0.0，总共迭代2次，正好迭代到二度好友位置，数据只在EdgeDirection.Both条件下发送，也就是只有两个顶点之间的边既是出度又是入度的情况下才会执行
    val cohesin = initialGraph.pregel(0.0, 2, EdgeDirection.Both)(
        //vprog方法，用来将发送来的数据和顶点中原来的属性中的第一个值(算作亲密度)进行简单合并
        (id,dist,newDist) => 
          dist.updated(0, (dist(0).toDouble+newDist)+"")
          ,
          //sendMsg方法，用来向下一个顶点发送消息
        triplet => { // Send Message   
          if (triplet.srcAttr(0).toDouble + triplet.attr < triplet.dstAttr(0).toDouble) {  
            Iterator((triplet.dstId, triplet.srcAttr(0).toDouble + triplet.attr))  
          } else {  
            Iterator.empty  
          }  
        },  
        //如果同一个顶点接收到两份甚至多份发来的数据，那么经过vprog方法后在这里再次进行聚合为一个值
      (a,b) => a+b // Merge Message  
    )
    cohesin.vertices
  }
}








