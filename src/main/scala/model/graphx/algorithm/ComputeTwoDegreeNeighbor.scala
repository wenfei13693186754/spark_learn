package com.wdcloud.Utils

import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.graphx._

/**
 * 使用pregel计算图的二跳邻居
 */
object ComputeTwoDegreeNeighbor {
  
  def computeTwoDegreeNeighbor(graph: Graph[Array[String], Double]): VertexRDD[Iterable[VertexId]] = {
    val two=2  //这里是二跳邻居 所以只需要定义为2即可
    val newG=graph.mapVertices((vid,_)=>Map[VertexId,Int](vid->two))
            .pregel(Map[VertexId,Int](), two, EdgeDirection.Out)(vprog, sendMsg, mergeMsg)
    
    //二次遍历后的数据
    newG.vertices.collect().foreach(println(_))
    //过滤得到二跳邻居 就是value=0 的顶点
    val twoJumpFirends=newG.vertices
    .mapValues(_.filter(_._2==0).keys)
    //将其打印出来    
    twoJumpFirends.collect().foreach(println(_))
    twoJumpFirends
  }
  
  type VMap=Map[VertexId,Int]
    
  /**
  * 节点数据的更新 就是集合的union
  */  
  def vprog(vid:VertexId,vdata:VMap,message:VMap)
  :Map[VertexId,Int]=mergeMsg(vdata,message)
  
  /**
  * 发送消息
  */
  def sendMsg(e:EdgeTriplet[VMap, _])={  
    
    //取两个集合的差集  然后将生命值减1
    //keySet:Collects all keys of this map in a set.Returns a set containing all keys of this map.
    //--:取两个集合的差集
    val srcMap=(e.dstAttr.keySet -- e.srcAttr.keySet).map { k => k->(e.dstAttr(k)-1) }.toMap
    val dstMap=(e.srcAttr.keySet -- e.dstAttr.keySet).map { k => k->(e.srcAttr(k)-1) }.toMap
    
    if(srcMap.size==0 && dstMap.size==0)
      Iterator.empty
    else
      Iterator((e.dstId,dstMap),(e.srcId,srcMap))
  }  
  
  /**
  * 消息的合并
  */
  def mergeMsg(spmap1: VMap, spmap2: VMap): VMap =
  (spmap1.keySet ++ spmap2.keySet).map {
    k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
  }.toMap
}