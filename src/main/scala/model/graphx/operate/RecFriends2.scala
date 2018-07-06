package com.wdcloud.graphx

import breeze.linalg.SparseVector
import model.graphx.operate.TimeOperate
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import scala.io.Source

/**
 * 使用spark-graphx实现二度好友的推荐
 */
object RecFriends2 {
  val projectDir = "E:\\spark\\Spark-GraphX\\data\\recBasedGraph\\test\\"
	val id = "relInGood6" //只建立这个ID对应的社交关系图
	//创建支持向量的二元搜索对象
	type Feature = breeze.linalg.SparseVector[Int]
  
	val conf = new SparkConf().setMaster("local[*]").setAppName("graphDemo")
	val sc = new SparkContext(conf)
	
  def main(args: Array[String]): Unit = {
    val t1 = System.currentTimeMillis()
    val graph = createGraph()
    val t2 = System.currentTimeMillis()
    println("创建图用时："+(t2-t1))
    //加入亲密度后推荐二度好友
    recTwoDegreeFriendsWithCohesion(graph)
    val t3 = System.currentTimeMillis()
    println("推荐好友用时："+(t3-t2))
    
    //未加入亲密度推荐二度好友
    //recTwoDegreeFriends(graph)
    //往顶点上添加属性
    //addAttrOnVertex(graph,Math.abs("person_1".hashCode().toLong),"猜猜我是谁？")
  }  
  
  /*
   * 创建图
   * Graph[Array[String], Double]   这里创建图使用了Graph类的单例对象的aply构造方法创建，返回的Graph中的Array[String]是vertices的attr的类型
   * Double是Edge上的属性的类型
   */
  def createGraph(): Graph[Array[String], Double] = {
    //***********************创建物属性向量*******************************************************
    
    //通过 .feat 文件,并将数据转化为由用户id和用户属性向量组成的map  
    val featureMap = Source.fromFile(projectDir + id + ".feat").getLines().
      map {
        line =>
          val row = line.split(" ")
          //注意：ID 不能之间当作 Long 型的时候 常常用 hashcode 代替
          val key = Math.abs(row.head.hashCode.toLong)
          
          //SparseVector-->此辅助构造函数假定索引数组已经排序
          //tail:select all elements except the first one
          val feat = SparseVector(row.tail.map(_.toInt))
          (key, feat)
      }.toMap

    //**************************创建图,图的每个边上放着物之间的共同特征数(包括人与人，人与物，物与物)********
    
    //调用时间操作工具类，用来算出用户与好友最近一次交互时间与当前时间的时间差
    val timeUtil = new TimeOperate()
    
    //创建一个累加器,用来计算读取到的数据行数
    val count = sc.accumulator(0)
    
    //通过 .edges 文件计算边，得到两个用户之间的关系 并且计算他们相同特征的个数
    val edgesData = sc.textFile(projectDir+id+".edges").map {
      line =>
        
        if (line.isEmpty()) {  
        	(0L,0L,0,0)
        }
        //累加器加1
        count.+=(1)
      	val row = line.split("\\|")
      	if (row.length < 3) {
      	  throw new IllegalArgumentException("Invalid line at "+count+"row, line is "+line+"  "+row(0).mkString(","))
      	}
        
        //计算出用户之间共同特征数
  			val ids = row(0).split(" ")
  			val srcId: Long = Math.abs(ids(0).hashCode.toLong)
  			val dstId: Long = Math.abs(ids(1).hashCode.toLong)
  			val srcFeat = featureMap(srcId)
  			val dstFeat = featureMap(dstId)
  			val numCommonFeats = srcFeat dot dstFeat
  			
  			//计算user和一度好友交流的总次数，包括点赞、聊天等
  			val communicateNumArray: Array[Int] = row(1).split(" ").map { x => x.toInt }
  			val communicateNum = communicateNumArray.aggregate(0)({ (sum,ch) => sum + ch}, { (p1,p2) => p1+p2})
  			
  			//计算user和一度好友的最近一次交互的时间与当前时间的差
  			val time = timeUtil.DealTime(row(2))
				(srcId,dstId,communicateNum,numCommonFeats,time.toLong)
    }
    
    //计算出user和一度好友的总的交互次数和总的共有的特征数量和总的交互的时间差的和
    val userData = edgesData.map(x => (x._3,x._4)).reduce( (x,y) => (x._1+y._1,x._2+y._2))
    //计算出user和一度好友的平均交互次数和平均共有特征数量
    val attrAvg: Double = userData._2/count.value//平均拥有的共同特征数
    val comAvg: Double = userData._1/count.value//平均交互次数
      
    //调用countCohesion方法计算user和一度好友之间的亲密度
    val finallyCohesion = countCohesion(edgesData,attrAvg,comAvg)
    //创建边  
    val edges = finallyCohesion.map(x => Edge(x._1,x._2,x._3))
    
  
    //通过.txt文件计算出图的顶点，顶点的一个属性是该顶点所属的类别，是人还是物
    val vertex = sc.textFile(projectDir+id+".attr").map { 
      line => 
        var srcId:Long = 0L
        var attr:Array[String] = null
        if (!line.isEmpty() && !line.startsWith("#")) {
        	val row = line.split("\\|")
        	if (row.length < 2) {
        	  throw new IllegalArgumentException("Invalid line: "+line)
        	}
        	srcId = Math.abs(row(0).hashCode().toLong)
    			attr = row(1).split(" ")
    			(srcId,attr)
        }else{
          (srcId,attr)
        }
    } 
      
    //利用 fromEdges建立图 
    Graph(vertex,edges).cache
  }
    
  
  /*
   * 推荐二度好友
   * 不考虑亲密度
   */
  def recTwoDegreeFriends(graph: Graph[Array[String], Double]){
    //查看一下具有3个相同特征的用户对
    print(graph.edges.filter(_.attr == 3).count())
    //val subGraph = graph.subgraph(epred = e => e)
    //************************不考虑用户亲密度，只考虑一度好友所共有的二度好友的数量的推荐*************************************
    
	  println("*******************************************************")  
	  //找到userId对应的vertex和它对应的一度好友顶点以及它们之间的边
	  val oneTriplet = graph.triplets.filter(x => x.srcId == Math.abs("person_1".hashCode().toLong))  
	  //输出user的一度好友
	  oneTriplet.foreach(x => println(x.srcId+" 的一度好友是："+x.dstId))
	  //获取到一度好友的id
    val oneFriendsId = oneTriplet.map(x => x.dstId).collect()   
	   
    //找到user对应的二度好友顶点和它的源顶点以及它们之间的边
    val twoTriplet = graph.triplets.filter(x => oneFriendsId.contains(x.srcId) && x.dstId !=Math.abs("person_1".hashCode().toLong) && x.dstAttr != "person" )
    //找到二度好友的id
    val recFriends = twoTriplet.map(x => (x.dstId,1)).reduceByKey((x,y) => x+y).sortBy(x => x._2,false).collect
    recFriends.foreach { x => println("二度好友id是："+x._1+" 数量是："+x._2) }
  }
   
  /*
   * 推荐二度好友
   * 考虑亲密度
   */
  def recTwoDegreeFriendsWithCohesion(graph: Graph[Array[String], Double]){
    println("*******************************************************")  
    val personId = Math.abs("person_1".hashCode().toLong)
	  //找到userId对应的vertex和它对应的一度好友顶点以及它们之间的边
	  val oneTriplet = graph.triplets.filter(x => x.srcId == personId)  
	  //获取到一度好友的id
    val oneFriendsId = oneTriplet.map(x => x.dstId).collect()     

    //找到user对应的二度好友顶点和它的源顶点以及它们之间的边
    val twoTriplet = graph.triplets.filter(x => oneFriendsId.contains(x.srcId) && x.dstId != personId && !oneFriendsId.contains(x.dstId))
    //找到二度好友的id
    val recFriends = twoTriplet.map(x => (x.dstId,1)).reduceByKey((x,y) => x+y)
    //recFriends.foreach { x => println("二度好友id是："+x._1+" 数量是："+x._2) }
    
    //*******************考虑到用户之间亲密度的推荐，用用户之间的共有属性来表述用户之间的亲密度************************************
    
    //用户与二度好友亲密度计算
    //RDD[(VertexId, (VertexId, Int))]-->一度好友id,二度好友id,一度好友与二度好友亲密度
    val oneFriendsToTwoFriendsIntimacy = twoTriplet.map(x => (x.srcId,(x.dstId,x.attr)))
    //RDD[(VertexId, Int)]-->一度好友id,user和一度好友亲密度
    val userToOneFriendsIntimacy = oneTriplet.map(x => (x.dstId,x.attr))
    //RDD[(VertexId, ((VertexId, Int), Int))]-->RDD[(一度好友id,((二度好友id,一度好友和二度好友亲密度)，user和一度好友亲密度))]
    val mixIntimacy = oneFriendsToTwoFriendsIntimacy.join(userToOneFriendsIntimacy)
    //user与二度好友的亲密度
    //RDD[(VertexId, Int)]-->(二度好友id,亲密度)
    val initialIntimacy = mixIntimacy.map{x => 
      val intimacyData = x._2
      val oneFriendsToTwoFriendsIntimacy = intimacyData._1._2
      val userToOneFriendsIntimacy = intimacyData._2
      (intimacyData._1._1,oneFriendsToTwoFriendsIntimacy*0.7+userToOneFriendsIntimacy*0.3)
    }
    
    //因为建立起来的user和二度好友的亲密度可能会有多个值，原因就是某个二度好友可能被多个一度好友所拥有，这样就会被计算多次
    //当然也应该计算多次，这里直接取多次亲密度之和作为user和该二度好友亲密度
    val userToTwoFriendsIntimacy = initialIntimacy.reduceByKey((x,y) => (x+y))
    
    userToTwoFriendsIntimacy.sortBy(x => x._2,false).take(10).foreach { x => println("加入亲密度后二度好友id是："+x._1+" 亲密度是："+x._2) }
     
  }
    
  /*
   * 给单个顶点上添加属性
   */
  def addAttrOnVertex(graph: Graph[Array[String], Double],vId : VertexId, addAttr : String){
    println(vId+"  "+addAttr)
    val nGraph = graph.mapVertices((id,attr) => if (id != vId) {attr} else(attr.:+(addAttr))) 
    val vertex = nGraph.vertices.filter(x => x._1 == vId)
    vertex.foreach(x => println(x._1+" 的属性是："+x._2.mkString(",")))
  }
  
  
  /*
   * 计算user和一度好友之间的亲密度
   * Vector[(Long, Long, Int, Int,Long)]
   * 				srcId,dstId,communicateNum,numCommonFeats,time 
   */
  def countCohesion (edgeData:RDD[(Long, Long, Int, Int, Long)],attrAvg: Double,comAvg: Double): RDD[(Long, Long, Double)] = {
		//edgeData.foreach(x => println(x._1+"与"+x._2+"交互次数是："+x._3+",共同特征数是："+x._4))

    //计算亲密度
    //cohesion = (scoreAttr + scoreCom)/math.pow(x._5/3600000,1.5)
    val cohesion = edgeData.map{ x => 
      var scoreAttr: Double = 0
      var scoreCom: Double = 0
      var totalScore: Double = 0
      if(attrAvg != 0 && comAvg != 0){
        scoreAttr = (x._4 - attrAvg)*0.4/attrAvg 
        scoreCom = (x._3 - comAvg)*0.6/comAvg
        //时间差的指数作为分母
        totalScore = (scoreAttr + scoreCom)/math.pow(x._5/3600000,1.5)
        //时间差的对数作为分母
        //totalScore = 1/math.log(x._5/3600000)
        //使用牛顿冷却定律,t代表的是小时数
        //val t = x._5/3600000
        //totalScore = 1*1/math.pow(math.E, 0.009*t)
      }else {
        totalScore = -1.0
      }
    (x._1,x._2,totalScore)
    }
    
    //cohesion.foreach { x => println(x._1+" 与 "+x._2+" 相似度是  "+x._3) }
    cohesion
  }
  
}













