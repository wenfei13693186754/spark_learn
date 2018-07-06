package com.wdcloud.graphx

import scala.io.Source
import scala.math._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import breeze.linalg.SparseVector
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import scala.tools.ant.sabbus.Make
import model.graphx.operate.TimeOperate

/**
 * 使用spark-graphx实现基于圈子的物品的推荐
 * a.首先通过读取.attr文件获取到用户及其所属的圈子；
	 b.通过圈子id找到指定圈子里边的用户；
   c.看圈子里边用户喜欢看什么，然后将所看的东西(例如图书)排序后推荐给用户
		用户喜好什么，这个属性也放在edges文件中
 */
object RecFriends3 {
  val projectDir = "E:\\spark\\Spark-GraphX\\data\\"
	val id = "recItemBasedCircleAndSimUser\\relInGood" //只建立这个ID对应的社交关系图
	//创建支持向量的二元搜索对象
	type Feature = breeze.linalg.SparseVector[Int]
  
	val conf = new SparkConf().setMaster("local[*]").setAppName("graphDemo")
	val sc = new SparkContext(conf)
	
  def main(args: Array[String]): Unit = {
    val t1 = System.currentTimeMillis()
    //创建图
    val graph = createGraph()
    val t2 = System.currentTimeMillis()
    println("创建图用时："+(t2-t1))
    //加入亲密度后推荐二度好友
    recTwoDegreeFriendsWithCohesion(graph)
    //val t3 = System.currentTimeMillis()
    //println("推荐好友用时："+(t3-t2))
    
    //未加入亲密度推荐二度好友
    //recTwoDegreeFriends(graph)
    //往顶点上添加属性
    //addAttrOnVertex(graph,Math.abs("person_1".hashCode().toLong),"猜猜我是谁？")
    
    //val t4 = System.currentTimeMillis()
    //基于圈子推物品
    //recItemBasedCircleData(graph)
    //val t5 = System.currentTimeMillis()
    //println("基于圈子的推荐用时："+(t5-t4))
    
    
    //动态的在已有的图上添加节点
    //这里给person_1节点添加节点1,2,3
    //测试结果：时间复杂度太高不适用
    /*val t6 = System.currentTimeMillis()
    val nGraph = addVertexIntoGraphx(graph,"E:\\spark\\Spark-GraphX\\data\\recItemBasedCircleData2\\addNodeData.txt")
    nGraph.triplets.collect().foreach(println)
    val t7 = System.currentTimeMillis()
    println("添加三个顶点用时："+(t7-t6))
    nGraph.triplets.collect().foreach(x => println("源顶点："+x.srcId+" 的属性是："+x.srcAttr+" 目的顶点："+x.dstId+"的属性是："+x.dstAttr+" 边属性是："+x.attr))
*/  
    //实时的基于圈子的推荐，圈子作为一个节点存在于图中
    //recItemBasedCircleData2(graph)
    
    //离线的对图中的所有圈子计算出这个圈子的热门商品，并将热门商品id结果存放到圈子顶点的上边，作为圈子顶点的一个属性存在
    val newGraph = createHotItemForEveryCircleOfTheGraph(graph)
    
    //基于离线的，已经计算好的圈子热门物品数据，给某个用户推荐物品
    val recItemBasedOnCircle = recItemForUserBasedOnCircleOutLine(newGraph,"person_1")
    
    //实现基于圈子和基于相似用户的综合推荐(理论上基于圈子的推荐结果会在基于相似用户的推荐结果前边，因为圈子里边的人多，累加效应比较大)
    val recItemBasedCircleAndSimUser =  recItemBasedCircelAndSimUser(newGraph,"person_1")
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
          
          //调用方法，给物品或者人id大标识
          val key = markId(row.head)
          
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
  			val srcId: Long = markId(ids(0))
  			val dstId: Long = markId(ids(1))
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
        val row = line.split("\\|") 
      	srcId = markId(row(0))
  			//对于圈子如果作为顶点的一个属性的话，使用下边代码
      	//attr = row.tail  
      	//对于圈子作为一个顶点存在的话，使用如下代码
      	attr = row(1).split(" ")
      	//println("##"+srcId+"  "+attr.mkString(","))
  			(srcId,attr)
    } 
    //利用 fromEdges建立图 
    val graph = Graph(vertex,edges).cache
    //全图操作，每个dst顶点向src顶点发送自己的id,最终每个用户顶点都可以收集到自己的邻居物顶点的id(包括圈子，物品等)
    val dealGraph = graph.aggregateMessages[Array[VertexId]](x => x.sendToSrc(Array(x.dstId)), _++_, TripletFields.All)
    //将收集到信息的顶点重新加入到原图上，使得图中对应顶点包含自己邻居节点的id这一属性
    val finallyGraph = graph.joinVertices(dealGraph)((id, oldCost, extraCost) => oldCost.+:(extraCost.mkString(" ")))
    graph
  }
  
  /*
   * 动态的在已有的图上添加节点
   * 性能太差，不适用。创建一个图用时38689，创建三个顶点用时达到了118797
   */
  def addVertexIntoGraphx (graph: Graph[Array[String], Double], path: String):Graph[Array[String], Double] = {
    val graph1 = GraphLoader.edgeListFile(sc, path)
    val graph2 = graph1.mapVertices((id, attr) => Array[String]())
    val graph3 = graph2.mapEdges(e => 0.0)
    val nGraph = Graph(
      graph.vertices.union(graph3.vertices),
      graph.edges.union(graph3.edges)
    ).partitionBy(RandomVertexCut).groupEdges((attr1,attr2) => attr1 + attr2)
    
    nGraph
  }
    
  /*
   * 基于用户所属的同一个圈子，给用户推荐圈子里边人所喜欢的物品
   * 圈子id属于用户的一个属性
   */
  def recItemBasedCircleData (graph: Graph[Array[String], Double]) {
    //首先读取出圈子文件，里边包含圈子id和圈子所包含的用户id
    val circleData = sc.textFile(projectDir+id+".circle").map { x => x.split("\\|") }
    
    //从图中读出给定用户id的所属圈子id
    val userVertex = graph.vertices.filter(x => x._1 == Math.abs("person_1".hashCode().toLong)).first()
    val userBelongCircleId = userVertex._2(1).split(" ")
     
    //算出user一共有几个圈子
    val circleCount = userBelongCircleId.size
    
    //从圈子文件中过滤出user所在的圈子及各个圈子包含的人
    val circleBelongToUser = circleData.filter { x => userBelongCircleId.contains(x(0)) }
    //找到user所属圈子里边的其它用户与user共有的圈子数
    val otherUserBelongToCircle = circleBelongToUser.map { x => x(1).split(" ") }.reduce((x, y) => x.++:(y))
    val otherUserBelongToCircleWithWeight = sc.parallelize(otherUserBelongToCircle.map { x => (x,1) }).reduceByKey((x, y) => x + y)
    //otherUserBelongToCircleWithWeight.foreach(x => println(x._1+"用户和person_1共有的圈子数是："+x._2))
    
    //计算圈子中其它user的value值：value = commonCircleCount/circleCount
    val otherUserBelongToCircleRank = otherUserBelongToCircleWithWeight.map(x => (Math.abs(x._1.hashCode().toLong), x._2/circleCount.toDouble))
    //otherUserBelongToCircleRank.foreach(x => println(x._1+"的与user共有圈子的占比："+x._2))
    
    //获取到其它用户id
    val otherUser = otherUserBelongToCircleRank.map(x => x._1).collect()
    
    //从图中找出圈子文件中过滤出的所有用户及其喜好的图书，加上各个用户的权重后经过排序给user进行推荐
    val edgeTripletOfUserAndItem = graph.triplets.filter(x => otherUser.contains(x.srcId) && x.dstAttr(0).split(" ").contains("book"))
    //val rankOfUserLikeData = edgeTripletOfUserAndItem.map(x => (x.dstId, 1)).reduceByKey((x, y) => x + y)
    //将过滤出来的圈子中的其它用户id和对应的物品id转化为： RDD[(Long, (Long, Double))]-->(userId,(itemId,用户对物品的喜好程度))
    val rankOfUserLikeData = edgeTripletOfUserAndItem.map(x => (x.srcId.toLong,(x.dstId.toLong,x.attr)))
    //rankOfUserLikeData.foreach(x => println(x._1+" 对物品："+x._2._1+" 的喜好程度是："+x._2._2))
    //对rankOfUserLikeData和user得分RDD  otherUserBelongToCircleRank做join操作，获取到如下结果：
    //RDD[(Long, ((Long, Double), Double))]-->[(srcId,((dstId,value),likeLevel))]经过map后：RDD[(Long, Double)]-->dstId,value
    val itemAndRank = rankOfUserLikeData.join(otherUserBelongToCircleRank).map(x => (x._2._1._1,x._2._2*x._2._1._2))
    //对各个item做最后的打分汇总
    val rank2 = itemAndRank.reduceByKey((x,y) => x+y)
    //rank2.foreach(x => println(x._1+"物品的得分是："+x._2))
  
  }
  

  /*
   * 基于用户所属的一个圈子，给用户推荐圈子里边人所喜欢的物品
   * 与recItemBasedCircleData不同之处是，这里的圈子是以图中的一个节点存在
   */
  def recItemBasedCircleData2 (graph: Graph[Array[String], Double]) {
    //1.先找到user的圈子，圈子id组成一个Array数组userCircleArray
    val subGraph = graph.triplets.filter(x => x.srcId == ("1"+Math.abs("person_1".hashCode())).toLong && (x.dstId+"").startsWith("2"))
    val userCircleArray = subGraph.map(x => x.dstId).collect()
    println("person_1的圈子是："+userCircleArray.mkString(","))
    //2.aggregateMessage方法聚合和user有共同圈子的用户的圈子信息
    val userCommonCircleCount = graph.aggregateMessages[Array[VertexId]](
        x => {if(userCircleArray.contains(x.dstId)){
          x.sendToSrc(Array(x.dstId))
        }},
        _ ++_,
        TripletFields.None    
    ).map(x => (x._1,x._2))
    
    //找到和user有共同圈子的用户和user共有圈子的数量
    val otherUserCircleNumWithUser = userCommonCircleCount.map(x => (x._1, (x._2.intersect(userCircleArray)).size))
    otherUserCircleNumWithUser.foreach(x => println(x._1+"和user共有的圈子数是："+x._2))
    
    //找到这些用户所喜好的物品
    //先找到这些用户的id组成的集合
    val otherUserIdArray = otherUserCircleNumWithUser.map(x => x._1).collect
    //过滤出其他用户和他们所喜爱的物品所组成的TripletRDD
    val otherUserAndItem = graph.triplets.filter(x => otherUserIdArray.contains(x.srcId) && (x.dstId+"").startsWith("3"))
    //RDD[(VertexId, （VertexId, Int）)]-->srcId(otherUser的id,（item的id，物品得分))
    val initItemRank = otherUserAndItem.map(x => (x.srcId,(x.dstId,x.attr)))
    //执行完join操作时候形成的RDD:RDD[(VertexId, (Int, (VertexId, Int)))]
    val itemRank = otherUserCircleNumWithUser.join(initItemRank).map(x => (x._2._2._1,x._2._1*x._2._2._2)).reduceByKey((x, y) => x+y).sortBy(x => x._2)
    itemRank.foreach(x => println("*******"+x))
    
  }
  
  
  /*
   * 离线的对图中的所有圈子计算出这个圈子的热门商品，并将热门商品id结果存放到圈子顶点的上边，作为圈子顶点的一个属性存在
   * 1.先使用subGraph生成一张只包含用户顶点和圈子顶点和物品顶点的子图
   * 2.使用aggregateMessages聚合消息，使得每个圈子上都可以聚合到属于这个圈子的用户的id。。。。
   * 			结果是Array[(VertexId, Array[VertexId])]，(圈子id, 圈子里边所包含的用户id)
   * 3.然后遍历数组，对遍历出的装了用户id的数组再次使用聚合函数，让数组中的对应id顶点再次发送消息给邻居节点，
   * 4.因为这次是限定了只是某个圈子的用户给它们各自的邻居发送消息，所以从收到消息的节点中就可以过滤出圈子里边人所直接喜欢的物品
   * 5.找到了圈子里边的人喜欢的物品，然后再看对哪个物品喜好的人多，进过排序后取前n个，作为这个圈子的热门商品
   * 6.最终形成结果是(圈子id，Array(热门商品id))
   */
  def createHotItemForEveryCircleOfTheGraph(graph: Graph[Array[String], Double]):Graph[Array[String], Double] = {
    //graph.triplets.foreach(x => println(x.srcId+" "+x.srcAttr.mkString(",")+"\\|"+x.dstId+" "+x.dstAttr.mkString(",")))
    //过滤出只有用户顶点、圈子顶点和用户的所喜欢的书籍顶点以及它们之间边的一张子图
    val subGraph = graph.subgraph(
        epred => (epred.dstId+"").startsWith("2") || (epred.dstId+"").startsWith("3"), 
        (id, attr) => attr.contains("person") || attr.contains("circle") || attr.contains("book"))
    //subGraph.triplets.foreach(x => println(x.srcId+"**"+x.dstId))
    //在各个圈子 ,结果是VertexRDD[Array[VertexId]]，实际上Array[VertexId]是由x._1-->VertexId顶点id，x._2-->Array[VertexId]圈子中用户id组成
    val userIdBelongToCircle = subGraph.aggregateMessages[Array[VertexId]](x => x.sendToDst(Array(x.srcId)), _++_, TripletFields.All)
    //过滤出圈子顶点，结果是Array[(VertexId, Array[VertexId])]，(圈子id, 圈子里边所包含的用户id)
    val circleIdAndUserId = userIdBelongToCircle.filter(x => (x._1+"").startsWith("2")).collect()
    //circleIdAndUserId.foreach(x => println(x._1+"***"+x._2.mkString(",")))
    
    //开始找各个圈子的热门物品，最终形成[圈子id,Array[物品id]]的格式
    val circleMappingItem = circleIdAndUserId.map{x =>
      //val t1 = System.currentTimeMillis()
      //使用聚合函数
      val infoAggre = subGraph.aggregateMessages[Array[VertexId]](y =>
        //限定只有srcId是这个圈子的用户的时候，才会发送消息给dstId节点，x._2是该圈子所包含的用户id所组成的数组
        if(x._2.contains(y.srcId)){
          //发送消息到dst节点
         y.sendToDst(Array(y.srcId))
        },
        //在各个节点对收到的信息进行字符串累加
      _++_,
      //限定消息只能沿着出度方向发送
      TripletFields.All)
      //val t2 = System.currentTimeMillis()
      //println("聚合用时："+(t2-t1))
      //过滤出book节点，并计算收到的数据的数量，作为排序的依据。。。。排序，取前3个物品
      val hotItemIds = infoAggre.filter(w => (w._1+"").startsWith("3")).map(u => (u._1,u._2.size)).sortBy(v => v._2,false).take(10).map(m => m._1)
      //val t3 = System.currentTimeMillis()
      //println("过滤用时："+(t3-t2))
      (x._1,hotItemIds)
    }
    
    circleMappingItem.foreach(x => println(x._1+"****"+x._2.mkString(",")))
    //将热门物品动态添加到圈子节点上
    val newGraph = addAttrOnVertex(graph,sc.parallelize(circleMappingItem))
    //newGraph.vertices.foreach(x => println(x._1+" 属性是："+x._2.mkString(",")))
    newGraph
  }
  
  //基于离线的，已经计算好的圈子热门物品数据，给某个用户推荐物品
  /* 
   * (0).将用户传进来的id转化为Long类型
  	 (1).全图操作，每个dst顶点向src顶点发送自己的id,最终每个用户顶点都可以收集到自己的邻居物顶点的id(包括圈子，物品等)(创建图的时候完成)
     (2).将收集到信息的顶点重新加入到原图上，使得图中对应顶点包含自己邻居节点的id这一属性(创建图的时候完成)
  	 (3).过滤出srcId是userId的triplets
  	 (4).过滤出circle的triplets
  	 (5).找到圈子的热门商品都添加到数组list中
  	 (6).从集合中排除用户已经看过的物品
  	 (7).因为数组中的商品会有重复的，所以对集合进行reduce操作；
  	 (8).对最终结果按照reduce累加评分进行推荐
   */
  var list: List[String] = List()
  def recItemForUserBasedOnCircleOutLine(newGraph: Graph[Array[String], Double], id: String): Array[(String, Int)] = {
    //将用户传进来的id转化为Long类型的
    val ids = markId(id)
    //过滤出srcId是userId的triplets
    val userTriplets = newGraph.triplets.filter(x => x.srcId == ids)
    //过滤出circle的triplets
    val circleTriplets = userTriplets.filter(x => (x.dstId+"").startsWith("2"))
    //找到圈子的热门商品都添加到数组list中
    circleTriplets.foreach{x => 
      list = list ::: ((x.dstAttr(2).split(" ")).toList)  
    }
    //println("1  "+list.mkString(","))
    //找到用户已经看过的商品
    val itemUserHave = userTriplets.filter(x => (x.dstId+"").startsWith("3")).map(x => x.dstId+"").collect
    list = list.filter { x => !itemUserHave.contains(x)}
    //println("2  "+list.mkString(","))
    //因为数组中的商品会有重复的，所以再对list进行reduce处理，之后进行排序操作后输出结果
    val recItem = sc.parallelize(list.map { x => (x, 1) }).reduceByKey((x, y) => x + y).sortBy(x => x._2, false).take(10)
    //recItem.foreach(x => println(x._1+" "+x._2))
    recItem
  }
  
  /*
   * 基于圈子推荐的结果和基于相似用户推荐的结果的 综合作为最终的推荐结果
   * 圈子推荐的结果已经有了，只需要获取到相似用户，然后找打相似用户有的但是用户没有的进行推荐
   * 最后将两者推荐结果进行综合，做出一个最终的推荐结果就可以了
   * 
   * vprog:首先，如果顶点是book，那么第一次收到初始化的消息时候，该顶点不做任何处理，如果是人，
   *      那么有两种情况：如果收到的是人顶点的id，那么不做处理，如果收到的是book的id，那么将该id加到数组的最后一位  
   * sendMsg:判断当前triplet的dstId如果是book，那么就发送本dstId给上级节点；如果当前triplet
   * 			的dstId是user，那么就给上一级发送它接收到放到数组最后一位的book的id;    
   * merge:用来聚合发到同一个顶点上的消息  
   */
  def recItemBasedCircelAndSimUser(graph: Graph[Array[String], Double],id: String){
    val ids = markId(id)
    //暂时把用户的直接好友作为其相似好友处理，所以找到相似好友，就直接收集邻居节点信息就可以了
    //val simUser = graph.vertices.foreach(x => println("图上顶点信息："+x._1+" "+x._2.mkString(",")))
    //计算二跳邻居，找到每个用户的二跳邻居
    //这里的第一次迭代把每个用户的自己直接相关的book的id发到了用户节点上；第二次迭代用户节点向它的上一级节点发送消息，发的是该用户第一次迭代得到的book的id消息。
    //所以形成的结构是用户顶点属性数组的倒数第一位是二度物品id,倒数第二位是自己直接关系的物品id。
    val sssp = graph.pregel(" ", 2, EdgeDirection.Out)( 
      (id, dist, newDist) => // Vertex Program
         dist.:+(newDist),
      triplet => { // Send Message:
        if ((triplet.dstId+"").startsWith("3")) {  
          Iterator((triplet.srcId, triplet.dstId+""))  
        } else {  
          Iterator((triplet.srcId, triplet.dstAttr.last))  
        }  
      },  
      (a,b) => (a+" "+b).trim() // Merge Message：
    )  
    
    //得到VertexRDD[Array[String]],它是由(VertexId, Array[String])组成的，分别是userId和user的attr
    val userAndAttrInTheGraph = sssp.vertices.filter(x => (x._1+"").startsWith("1"))
    //计算user顶点上保存的直接关心的物品和二跳物品
    val userAndTwoDegreeItem = userAndAttrInTheGraph.mapValues(x => x.takeRight(2))
    //计算二跳物品并进行处理
    val recItemBasedSimUser = userAndTwoDegreeItem.mapValues{x => 
      //获取到user的直接关系的物品数组
      val userItem = x.head.split(" ")
      //获取用户的二跳物品
      val twoItem = x.tail.head.split(" ")
      //从二跳物品中过滤掉用户已经有直接关系的物品
      twoItem.filter { x => !userItem.contains(x) }
    }.mapValues{x => 
      val arr = x.map { x => (x,1)}
      val rdd = sc.parallelize(arr)
      val rec = rdd.reduceByKey((x, y) => x+y).collect()
      val recItem = rec.sortBy(x => x._2)
      recItem.takeRight(10)
    }
    
    recItemBasedSimUser.foreach(x => println(x._1+" "+x._2.mkString(",")))
    
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
	  val oneTriplet = graph.triplets.filter(x => x.srcId == ("1"+Math.abs("person_1".hashCode())).toLong)  
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
   * 给单个顶点上添加属性,属性是一个数组
   */
  def addAttrOnVertex(graph: Graph[Array[String], Double],table: RDD[(VertexId, Array[VertexId])]):Graph[Array[String], Double] = {
    graph.joinVertices(table)((id,attr,addAttr) => attr.:+(addAttr.mkString(" ")))
  }
  
  /*
   * 给单个顶点上添加属性，属性是单个字符串
   */
  def addAttrOnVertex(graph: Graph[Array[String], Double],vId: VertexId, addAttr: String): Graph[Array[String], Double] = {
    graph.mapVertices((id,attr) => if (id != vId) {attr} else(attr.:+(addAttr))) 
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
      }else {
        totalScore = -1.0
      }
    (x._1,x._2,totalScore)
    }
    
    //cohesion.foreach { x => println(x._1+" 与 "+x._2+" 相似度是  "+x._3) }
    cohesion
  }
  
  /*
   * 标识不同物品id的工具方法
   */
  def markId(id: String):Long = {
    //为了区分不同的物品(圈子，书籍等)和人，这里要给不同的物品id添加唯一标识，用来区分不同的物品
    //注意：ID 不能之间当作 Long 型的时候 常常用 hashcode 代替
    val key = id.substring(0, 4) match {
      case "pers" => ("1"+Math.abs(id.hashCode)).toLong
      case "circ" => ("2"+Math.abs(id.hashCode)).toLong
      case "book" => ("3"+Math.abs(id.hashCode)).toLong
      case "movi" => ("4"+Math.abs(id.hashCode)).toLong
      case _      => ("5"+Math.abs(id.hashCode)).toLong
    }
    //println(id+"  "+key)
    key
  }
  
}













