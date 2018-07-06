package com.wdcloud.Utils

import org.apache.spark.graphx._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*
 * 使用两次遍历，首先进行初始化的时候将自己的生命值设为2，第一次遍历向邻居节点传播自身带的ID以及生命值为1(2-1)的消息，
 * 第二次遍历的时候收到消息的邻居再转发一次，生命值为0，最终汇总统计的时候 只需要对带有消息为0ID的进行统计即可得到二跳邻居
 */
object pregelErTiaoNeibor {
	type VMap=Map[VertexId,Int]
  def main(args: Array[String]): Unit = {
	  val conf = new SparkConf().setAppName("demo").setMaster("local[*]")
	  val sc=new SparkContext(conf);
    val edgeRdd = sc.textFile("E:\\spark\\Spark-GraphX\\data\\recItemBasedCircleAndSimUser\\1.txt").map(x=>{
      val data = x.split(" ")
      Edge(data(0).toLong,data(1).toLong,None)
    })
    println(edgeRdd.count())
    val edges = edgeRdd.sample(false, 0.1, 1)
    println(edgeRdd.count()+"|||"+edges.count())
    //构建边的rdd
    /*val edgeRdd=sc.parallelize(edge).map(x=>{
      Edge(x._1.toLong,x._2.toLong,None)
    })*/
    
    //构建图 顶点Int类型
    val g=Graph.fromEdges(edges, 0)
    
    val two=2  //这里是二跳邻居 所以只需要定义为2即可
    val newG=g.mapVertices((vid,_)=>Map[VertexId,Int](vid->two))//初始化生命值为2
                .pregel(Map[VertexId,Int](), two, EdgeDirection.Out)(vprog, sendMsg, addMaps)

    //newG.vertices.collect().foreach(println(_))
    //过滤得到二跳邻居 就是value=0 的顶点
    val twoJumpFirends=newG.vertices
    .mapValues(_.filter(_._2==0).keys)
    twoJumpFirends.collect().foreach(println(_))

  }
  /**
   * 节点数据的更新 就是集合的union
   */
  def vprog(vid:VertexId,vdata:VMap,message:VMap)
  :Map[VertexId,Int]=addMaps(vdata,message)
  
  /**
   * 发送消息
   */
  def sendMsg(e:EdgeTriplet[VMap, _])={
	  //取两个集合的差集  然后将生命值减1
    //keySet:Collects all keys of this map in a set.
		println("**"+e.srcAttr)
	  val srcMap=(e.dstAttr.keySet -- e.srcAttr.keySet).map { k => k->(e.dstAttr(k)-1) }.toMap
	  val dstMap=(e.srcAttr.keySet -- e.dstAttr.keySet).map { k => k->(e.srcAttr(k)-1) }.toMap
	  if(srcMap.size==0 && dstMap.size==0)
		  Iterator.empty
	  else
		  Iterator((e.dstId,dstMap),(e.srcId,srcMap))
  }  
  
  /**
   * 消息的合并
   * 第一次迭代：
   * 		1.每个顶点收到的是各个邻居节点发来的带有自己id和生命值的一个Map[VertexId, Int]，然后使用addMaps函数进行合并消息；
   * 		2.合并的时候先使用 ++ 函数将两个map使用keySet函数收集每个map中的key形成一个set集合，然后进行关联(合并并去重)，形成一个包含多个元素id的set；
   * 		3.然后使用map函数遍历set集合，遍历过程中判断spmap1或者spmap2中是否有该key值，有的话，拿出对应的value值(也就是该id对应的生命值)，没有的话返回Int.MaxValue
   * 		4.最后从获得的value值中拿到最小的作为新的value,与k一起组成新的键值对。
   * 第二次迭代：
   * 		1.每个顶点将第一次迭代到的数据转发到邻居节点；
   * 		2.重复执行第一次迭代中的2-4步。
   * 注：因为这里会存在A顶点既是B顶点的一度邻居节点，又是B顶点的二度邻居节点，所以需要在第二次迭代的时候从顶点中即将发送出去的消息中去除掉和目的顶点重合的（VertexId,Int）,这个事由sendMsg来做
   */
  def addMaps(spmap1: VMap, spmap2: VMap): VMap =
		  (spmap1.keySet ++ spmap2.keySet).map {  
    //Returns the value associated with a key, or a default value if the key is not contained in the map.
	  k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
  }.toMap
}