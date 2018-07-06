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
 * 使用spark-graphx实现基于好友、圈子来实现推荐二度好友、圈子、物品
 * 		要求：1.离线的、全量的；
 * 				 2.各种关系基于不同权重进行推荐
 *
 */
object RecFriends4 {
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
    //val newGraph = createHotItemForEveryCircleOfTheGraph(graph)
    
    //基于离线的，已经计算好的圈子热门物品数据，给某个用户推荐物品
    //val recItemBasedOnCircle = recItemForUserBasedOnCircleOutLine(newGraph,"person_1")
    
    //实现基于圈子和基于相似用户的综合推荐(理论上基于圈子的推荐结果会在基于相似用户的推荐结果前边，因为圈子里边的人多，累加效应比较大)
    //val recItemBasedCircleAndSimUser =  recItemBasedCircelAndSimUser(newGraph,"person_1")
    
    recBasedSimUser(graph)
  }

  /*
   * 创建图
   * Graph[Array[String], Double]   这里创建图使用了Graph类的单例对象的aply构造方法创建，返回的Graph中的Array[String]是vertices的attr的类型
   * Double是Edge上的属性的类型
   */
  def createGraph(): Graph[Array[String], String] = {
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
    val edgesData = sc.textFile(projectDir + id + ".edges").map {
      line =>

        if (line.isEmpty()) {
          (0L, 0L, 0, 0)
        }
        //累加器加1
        count.+=(1)
        val row = line.split("\\|")
        if (row.length < 3) {
          throw new IllegalArgumentException("Invalid line at " + count + "row, line is " + line + "  " + row(0).mkString(","))
        }

        //计算出用户之间共同特征数
        val ids = row(0).split(" ")
        val srcId: Long = markId(ids(0))
        val dstId: Long = markId(ids(1))
        val srcFeat = featureMap(srcId)
        val dstFeat = featureMap(dstId)
        val numCommonFeats = srcFeat dot dstFeat

        //计算user和一度好友交流的总次数，包括点赞、聊天等
        val communicateNumArray: Array[Int] = row(2).split(" ").map { x => x.toInt }
        val communicateNum = communicateNumArray.aggregate(0)({ (sum, ch) => sum + ch }, { (p1, p2) => p1 + p2 })

        //计算user和一度好友的最近一次交互的时间与当前时间的差
        val time = timeUtil.DealTime(row(3))
        //这里的ids(2)是两个顶点之间的关系，比如是好友还是关注
        (srcId, dstId,row(1)+":"+20)
    }

    //计算出user和一度好友的总的交互次数和总的共有的特征数量和总的交互的时间差的和
    //val userData = edgesData.map(x => (x._4, x._5)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    //计算出user和一度好友的平均交互次数和平均共有特征数量
    //val attrAvg: Double = userData._2 / count.value //平均拥有的共同特征数
    //val comAvg: Double = userData._1 / count.value //平均交互次数

    //调用countCohesion方法计算user和一度好友之间的亲密度
    //val finallyCohesion = countCohesion(edgesData, attrAvg, comAvg)
    //创建边  
    val edges = edgesData.map(x => Edge(x._1, x._2, x._3))

    //通过.txt文件计算出图的顶点，顶点的一个属性是该顶点所属的类别，是人还是物
    val vertex = sc.textFile(projectDir + id + ".attr").map {
      line =>
        var srcId: Long = 0L
        var attr: Array[String] = null
        val row = line.split("\\|")
        srcId = markId(row(0))
        //对于圈子如果作为顶点的一个属性的话，使用下边代码
        //attr = row.tail  
        //对于圈子作为一个顶点存在的话，使用如下代码
        attr = row(1).split(" ")
        //println("##"+srcId+"  "+attr.mkString(","))
        (srcId, attr)
    }
    //利用 fromEdges建立图 
    val graph = Graph(vertex, edges).cache
    //全图操作，每个dst顶点向src顶点发送自己的id,最终每个用户顶点都可以收集到自己的邻居物顶点的id(包括圈子，物品等)
    val dealGraph = graph.aggregateMessages[Array[VertexId]](x => x.sendToSrc(Array(x.dstId)), _ ++ _, TripletFields.All)
    //将收集到信息的顶点重新加入到原图上，使得图中对应顶点包含自己邻居节点的id这一属性
    val finallyGraph = graph.joinVertices(dealGraph)((id, oldCost, extraCost) => oldCost.+:(extraCost.mkString(" ")))
    graph
  }

  /*
   * 动态的在已有的图上添加节点
   * 性能太差，不适用。创建一个图用时38689，创建三个顶点用时达到了118797
   */
  def addVertexIntoGraphx(graph: Graph[Array[String], Double], path: String): Graph[Array[String], Double] = {
    val graph1 = GraphLoader.edgeListFile(sc, path)
    val graph2 = graph1.mapVertices((id, attr) => Array[String]())
    val graph3 = graph2.mapEdges(e => 0.0)
    val nGraph = Graph(
      graph.vertices.union(graph3.vertices),
      graph.edges.union(graph3.edges)).partitionBy(RandomVertexCut).groupEdges((attr1, attr2) => attr1 + attr2)

    nGraph
  }

 
  /*
   * 基于一度好友推荐好友、物、圈子
   */
  var count = 2
  def recBasedSimUser(graph: Graph[Array[String], String]) {
    val pGraph = graph.pregel(" ", 2, EdgeDirection.Both)(
      vprog,
      sendMsg,
      mergeMsg
    )
    val vertexAndAttr = pGraph.vertices.mapValues { x => x.apply(5) }.filter(x => (x._1+"").startsWith("1"))
    //vertexAndAttr.foreach(x => println(x._1+"的属性是："+x._2))
  }

  /*
   * pregel的vprog函数
   */
  def vprog(id: VertexId, dist: Array[String], newDist: String): Array[String] = {
    dist.update(6, (dist(6).toInt - 1)+"")
    //第一次迭代:dstId1:dstId2|关系：num
    if(dist(5).length() == 1){
    	dist.update(5, newDist)
    	dist.clone()
    }else{//第二次迭代:srcId:dstId1:dstId2|关系1:num2|关系2:num1
      val data = newDist.split(" ")
		  //获取到第一次迭代的数据，准备与第二次迭代的数据进行差集计算
      //dstId1:dstId2|关系：num
		  val itera1 = (dist(5).split(" ")).map { x => 
		    val arr1 = x.split("\\|") 
		    val arr2 = arr1(0).split(":")
		    arr2(1)
      }
      println("dist(5): "+dist(5))
      println("dist: "+dist.mkString(","))
      println("itera1: "+itera1.mkString(","))
      println("newDist: "+newDist)
		  //过滤掉第一次迭代的数据
		  val data2 = data.filter { x => 
		    val data1 = x.split(":")
		    if(itera1.contains(data1(2))){
		      false
		    }else{
		      true
		    }
      }
		  
      //对得到的二次迭代的数据进行处理：
		  //处理后的格式：Map[String, Array[(String, String)]]
      val data3 = data2.map { x => 
        //使用“:”拆分每条数据，目的是得到srcId,然后使用srcId进行分组，
        //最终形成：id:((srcId:dstId1:dstId2|关系1:num2|关系2:num1),(srcId:dstId1:dstId2|关系1:num2|关系2:num1)...) 
        val data4 = (x.split(":"))(0)
        (data4,x)
      }.groupBy(x => x._1)
     val data5 = data3.map(x => (x._1,x._2.mkString(" ")))
     dist.update(5, data5.mkString)
     dist
    }
  }
  
  /*
   * pregel的sendMsg
   * 	第一次迭代发送的数据
    		dstId1:dstId2|关系：num
    	第二次迭代发送数据
    		srcId:dstId1:dstId2|关系1:关系2|num1:num2
   */
  def sendMsg(triplet: EdgeTriplet[Array[String], String]): Iterator[(VertexId, String)] = {
	  //每次调用sendMsg方法，都对相应的dstId顶点上的属性6减一，用来表示这个顶点迭代的次数
    triplet.dstAttr.update(6, (triplet.dstAttr(6).toInt - 1)+"")
    if(triplet.dstAttr(6).toInt == 1){//第一次迭代
      println("sendMsg1发送的数据： "+triplet.srcId+":"+triplet.dstId +"|"+ triplet.attr)
    	Iterator((triplet.srcId, triplet.srcId+":"+triplet.dstId +"|"+ triplet.attr))
    }else{//第二次迭代
      val itera1 = triplet.dstAttr(5)
      val str0 = itera1.split(" ")
      val str3 = str0.map { x => 
        val str1 = x.replaceFirst("\\|", "\\|"+triplet.attr+"\\|")
        val str2 = triplet.srcId+":"+str1//srcId:dstId1:dstId2|关系1:num2|关系2:num1
        str2
      }
      println("获取第一次迭代后放进去的属性："+itera1)
      println("sendMsg2发送的数据： "+str3.mkString(" "))
    	Iterator((triplet.srcId, str3.mkString(" ")))
    } 
  }
  
  /*
   * pregel的mergeMsg
   */
  def mergeMsg(a: String,b: String): String = {
    a+" "+b
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
  def createHotItemForEveryCircleOfTheGraph(graph: Graph[Array[String], String]):Graph[Array[String], String] = {
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
  def recItemForUserBasedOnCircleOutLine(newGraph: Graph[Array[String], String], id: String): Array[(String, Int)] = {
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
  def recItemBasedCircelAndSimUser(graph: Graph[Array[String], String],id: String){
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
   * 给单个顶点上添加属性,属性是一个数组
   */
  def addAttrOnVertex(graph: Graph[Array[String], String], table: RDD[(VertexId, Array[VertexId])]): Graph[Array[String], String] = {
    graph.joinVertices(table)((id, attr, addAttr) => attr.:+(addAttr.mkString(" ")))
  }

  /*
   * 给单个顶点上添加属性，属性是单个字符串
   */
  def addAttrOnVertex(graph: Graph[Array[String], Double], vId: VertexId, addAttr: String): Graph[Array[String], Double] = {
    graph.mapVertices((id, attr) => if (id != vId) { attr } else (attr.:+(addAttr)))
  }

  /*
   * 计算user和一度好友之间的亲密度
   * Vector[(Long, Long, Int, Int,Long)]
   * 				srcId,dstId,communicateNum,numCommonFeats,time 
   */
  def countCohesion(edgeData: RDD[(Long, Long, String, Int, Int, Long)], attrAvg: Double, comAvg: Double): RDD[(Long, Long, String)] = {
    //edgeData.foreach(x => println(x._1+"与"+x._2+"交互次数是："+x._3+",共同特征数是："+x._4))

    //计算亲密度
    //cohesion = (scoreAttr + scoreCom)/math.pow(x._5/3600000,1.5)
    val cohesion = edgeData.map { x =>
      var scoreAttr: Double = 0
      var scoreCom: Double = 0
      var totalScore: Double = 0
      if (attrAvg != 0 && comAvg != 0) {
        scoreAttr = (x._5 - attrAvg) * 0.4 / attrAvg
        scoreCom = (x._4 - comAvg) * 0.6 / comAvg
        //时间差的指数作为分母
        totalScore = (scoreAttr + scoreCom) / math.pow(x._6 / 3600000, 1.5)
      } else {
        totalScore = -1.0
      }
      (x._1, x._2, x._3+""+totalScore)
    }

    //cohesion.foreach { x => println(x._1+" 与 "+x._2+" 相似度是  "+x._3) }
    cohesion
  }

  /*
   * 标识不同物品id的工具方法
   */
  def markId(id: String): Long = {
    //为了区分不同的物品(圈子，书籍等)和人，这里要给不同的物品id添加唯一标识，用来区分不同的物品
    //注意：ID 不能之间当作 Long 型的时候 常常用 hashcode 代替
    val key = id.substring(0, 4) match {
      case "pers" => ("1" + Math.abs(id.hashCode)).toLong
      case "circ" => ("2" + Math.abs(id.hashCode)).toLong
      case "book" => ("3" + Math.abs(id.hashCode)).toLong
      case "movi" => ("4" + Math.abs(id.hashCode)).toLong
      case _      => ("5" + Math.abs(id.hashCode)).toLong
    }
    //println(id+"  "+key)
    key
  }

}













