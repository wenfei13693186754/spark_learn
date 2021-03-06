package com.wdcloud.graphx

import org.apache.spark.graphx._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.Serializable

/**
 * 实现基于圈子的对所有用户的全量推荐，包括二度好友、二度物品、二度圈子
 */
class pregelCompute(sc: SparkContext) extends Serializable{
    def pregelDemo(graph: Graph[Array[String], Double]){
    
    val two=2  //这里是二跳邻居 所以只需要定义为2即可
    val newG=graph.pregel(" ", two, EdgeDirection.Out)(vprog, sendMsg, addMaps)
    
    val twoJumpFirends=newG.vertices.foreach(x => println(x._1+" "+x._2.mkString(","))) 
    
  }   

  def vprog(vid:VertexId,vdata:Array[String],message:String)
  :Array[String]={
    vdata.update(6, (vdata(6).toInt-1)+"")
    //对推荐结果按照人、圈子、物品进行分组前预处理，形成Array[(String, String)]格式，例如(book,364674614|book:-2.0)
    //1.要将不同子节点发来的同一种id的二跳节点数据进行处理，也就是堆同一种二跳节点的得分进行叠加
    //2.分组，按照物品、圈子、人进行分组，得到最终结果Map(book->(),circle->(),person->())
    if(vdata(6).toInt == -1 && !message.trim().isEmpty()){
      println("message: "+message)
      //获取到只包含顶点id，顶点类别，顶点得分的数据
      val middleData = (message.split(" ")).map { x => 
        val str = x.substring(1, x.length()-1)
        val data = str.split(",")
        val data1 = data(1).split("\\|")
        val data2 = data1(1).split(":")
        (data1(0),(data(0),data2(1)))  
      }
      
      //对相同id数据的得分进行合并
      val reduceByKeyData = sc.parallelize(middleData).reduceByKey((x, y) => (x._1,(x._2.toDouble+y._2.toDouble)+""))
      //对合并后的 数据按照不同类别进行分组
      val groupByClassData = reduceByKeyData.map(x => (x._2._1,x._1,x._2._2)).groupBy(x => x._1).collect.mkString(" ")
      vdata.update(5, groupByClassData)
      vdata.clone()
    }else{
      vdata.update(5, message)
      vdata.clone()
    }
  }
  
  def sendMsg(triplet:EdgeTriplet[Array[String], Double]): Iterator[(Long, String)] ={
	  //取两个集合的差集  然后将生命值减1
    //keySet:Collects all keys of this map in a set.
	   //每次调用sendMsg方法，都对相应的dstId顶点上的属性6减一，用来表示这个顶点迭代的次数
    if(triplet.srcAttr(6).toInt == 1){//第一次迭代:id1|类别属性:score1
    	Iterator((triplet.srcId, triplet.dstId +"|"+ triplet.dstAttr(0)+":"+triplet.attr))
    }else{//第二次迭代:id2|类别属性:score1
    	//println("dstAttr: "+triplet.dstAttr.mkString(","))
      val itera1 = triplet.dstAttr(5)//取出第一次迭代放进数组中的数据
      var recResultCount: Array[String] = Array[String]("") 
      if(itera1.length() == 1){
        Iterator((triplet.srcId, "")) 
      }else{
   		  val str0 = itera1.split(" ")
   		  //开始进行去重
        val srcAttr5 = (triplet.srcAttr(5).split(" ")).map { x => (x.split("\\|"))(0) }
        val neiborOfTwo = str0.filter { x => !srcAttr5.contains((x.split("\\|"))(0)) }
        //如果user和一度好友的好友一样，也就是经过过滤后得到的二跳邻居数是0，那么直接发回去空字符串
        if(neiborOfTwo.length == 0 || neiborOfTwo(0).trim().length() == 0){
          Iterator((triplet.srcId, "")) 
        }else{
        //对数据进行处理，对score进行叠加
        /*recResultCount = neiborOfTwo.map { x => 
          val dataIter1 = (x.split(":"))
          val oldScore = dataIter1.last.toDouble
          val finallyScore = oldScore + triplet.attr.toDouble 
          dataIter1.update(1, finallyScore+"")
          dataIter1.mkString(":")
        }*/
        
        //对推荐结果按照人、圈子、物品进行分组前预处理，形成Array[(String, String)]格式，例如(book,364674614|book:-2.0)
        val recResultGroup = neiborOfTwo.map { x => (x.substring(x.indexOf("|")+1, x.indexOf(":")),x) }
        Iterator((triplet.srcId, recResultGroup.mkString(" ")))
        }
      }
    }
  }  
  
  def addMaps(spmap1: String, spmap2: String): String ={
		  (spmap1+" "+spmap2).trim()
   }
}







