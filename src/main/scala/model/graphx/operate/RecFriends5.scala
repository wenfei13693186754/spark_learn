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
import java.io.Serializable
import com.google.common.hash.Hashing
import model.graphx.operate.TimeOperate

/**
 * 使用spark-graphx实现基于好友、圈子来实现推荐二度好友、圈子、物品
 * 		要求：1.离线的、全量的；
 * 				 2.各种关系基于不同权重进行推荐
 *
 */
class RecFriends5{}
object RecFriends5 extends Serializable{
  val projectDir = "E:\\spark\\Spark-GraphX\\data\\"
  val id = "recFriendsItemAndCircle\\relInGood" //只建立这个ID对应的社交关系图
  //创建支持向量的二元搜索对象
  type Feature = breeze.linalg.SparseVector[Int]  
  val conf = new SparkConf().setMaster("local[*]").setAppName("graphDemo")
  conf.registerKryoClasses(Array(classOf[com.wdcloud.graphx.RecFriends5], classOf[com.wdcloud.graphx.pregelCompute]))
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val t1 = System.currentTimeMillis()
    //创建图
    val graph = createGraph()
    val t2 = System.currentTimeMillis()
    println("创建图用时："+(t2-t1))
    
//******************************推荐*****************************************    
    //基于好友的推荐，包括推荐好友、物品和圈子，返回值类型是;RDD[(String, String, Map[String, List[(String, Double)]])]
    val recBasedFriends = recBasedSimUser(graph)

    //val hbase = new HbaseOperate()
    //hbase.writeDataToHbase(pregelGraph)

    //离线的对每个用户基于圈子进行物品、好友和圈子的推荐,返回值是RDD[(String, String, Map[String, List[(String, Double)]])]
    val recBasedCircle = recICUForUserBasedCircle1(graph)
    
    //对两种推荐结果进行合并
    combineResult("g1", recBasedFriends, recBasedCircle)
//********************************将结果写入kafka******************************************
    
  }

  /*
   * 创建图
   * Graph[Array[String], Double]   这里创建图使用了Graph类的单例对象的aply构造方法创建，返回的Graph中的Array[String]是vertices的attr的类型
   * Double是Edge上的属性的类型
   */
  def createGraph(): Graph[Map[String, Object], Double] = {

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
        
  			val ids = row(0).split(" ")
  			//生成srcId
  			val srcArr = ids(0).split(":") 
  			val srcId = hashId(srcArr(1), srcArr(0))
  			//生成dstId
  			val dstArr = ids(1).split(":")
  			val dstId = hashId(dstArr(1), dstArr(0))
  			//计算user和一度好友交流的总次数，包括点赞、聊天等
  			val communicateNumArray: Array[Int] = row(1).split(" ").map { x => x.toInt }
  			val communicateNum = communicateNumArray.aggregate(0)({ (sum,ch) => sum + ch}, { (p1,p2) => p1+p2})
  			//计算user和一度好友的最近一次交互的时间与当前时间的差
  			val time = timeUtil.DealTime(row(3))
				(srcId,dstId,communicateNum,time.toLong)
    }
    
    //计算出user和一度好友的总的交互次数和总的交互的时间差的和
    val userData = edgesData.map(x => (x._3,x._4)).reduce( (x,y) => (x._1+y._1,x._2+y._2))
    //计算出user和一度好友的平均交互次数和平均共有特征数量
    val comAvg: Double = userData._1/count.value//平均交互次数
    //调用countCohesion方法计算user和一度好友之间的亲密度
    val finallyCohesion = countCohesion(edgesData,comAvg)
    //创建边  
    val edges = finallyCohesion.map(x => Edge(x._1,x._2,x._3))

    //通过.txt文件计算出图的顶点，顶点的一个属性是该顶点所属的类别，是人还是物
    val vertex = sc.textFile(projectDir + id + ".attr").map {
      line =>
        var srcId: Long = 0L
        val row = line.split("\\|")
        val arr = row(1).split(" ")
        srcId = hashId(arr(0), row(0))
        //对于圈子作为一个顶点存在的话，使用如下代码
        //将属性添加到map集合中
        //person_1|person girl 25 beijing rose 1 2 邻居id 
        var attr: Map[String, Object] = Map("type" -> arr(0),"businessId" -> row(0), "gender" -> arr(1), "年龄" -> arr(2), "住址" -> arr(3), "姓名" -> arr(4),
            "life" -> arr(6), "other" -> arr(7))
        (srcId, attr)
    }
    //利用 fromEdges建立图 
    val graph = Graph(vertex, edges).cache
    //全图操作，每个dst顶点向src顶点发送自己的id,最终每个用户顶点都可以收集到自己的邻居物顶点的id(包括圈子，物品等)
    val dealGraph = graph.aggregateMessages[Array[VertexId]](x => x.sendToSrc(Array(x.dstId)), _ ++ _, TripletFields.All)
    //将收集到信息的顶点重新加入到原图上，使得图中对应顶点包含自己邻居节点的id这一属性
    val finallyGraph = graph.joinVertices(dealGraph)((id, oldCost, extraCost) => oldCost.updated("neiborId", extraCost.mkString(" ")))
    finallyGraph
  }


  /*
   * 基于圈子对用户进行全量推荐好友、物品和圈子
   * 步骤如下：首先所有顶点发送信息给src顶点，也就是user顶点，user顶点收到消息后将该消息以kv形式保存到map属性中；然后第二次迭代user顶点发送它们收到的消息和它们自己的属性给圈子顶点，
   * 圈子顶点收到消息后做去重和排序后以kv形式保存到map属性中；最后第三次迭代圈子顶点将收到的消息发送到user顶点。user顶点收到消息后从消息中去除掉自己直接关系的顶点信息，作为最终的推荐结果。
   * 其间要将图上的hash后的id值转化为业务id后作为处理结果返回。
   * 详细如下
   * 1.使用pregel，初始化信息使用每次迭代发送的消息格式，是Array[(VertexId, (String, Double,String))]()，一个空的数组，迭代三次，消息发送方向是任意(either)
   * 		1.初始化的时候，给每个顶点做标记“three”,表示进行初始化了，每个顶点生命值是three
   * 		2.第一次迭代，向src顶点，也就是用户顶点，发送dst的属性，格式是Array[(VertexId, (String, Double,String))]()。发送的时候sendMsg限定各个顶点的生命值是three，用来确定是第一次迭代，
   * 然后vprog函数先判断当前顶点生命值是three，以确定是第一次迭代，然后在进行消息聚合后，将用户顶点生命值改为two;
   * 		3.第二次迭代，sendMsg限定src顶点生命值是two，dst顶点生命值是three，并且限定dst顶点类型是circle，作用是只向圈子顶点发送消息；然后发送Array[(VertexId, (String, Double))]()
   * 格式的第一次迭代时候发送到各个src顶点的消息；vprog函数先判断是第二次迭代，然后聚合消息，并将顶点生命值减1；
   * 		4.第三次迭代，sendMsg先判断是第三次迭代，然后将第二次迭代收到的消息发送到src顶点；然后vprog函数判断是第三次迭代，之后将数据进行去重，分组放到属性map中
   * 2.对保存到每个用户顶点上的推荐结果，去除掉用户直接关系的顶点，作为最终的推荐结果。
   * 
   * 3.返回的结果：RDD[(String, String, Map[String, List[(String, Double)]])]-->RDD[(命名空间名称, 用户的业务id, Map[推荐物品类型, List[(推荐物品业务id, score)]])]
   */
  def recICUForUserBasedCircle1(graph: Graph[Map[String, Object], Double]):Array[(String, Map[String, List[(String, Double)]])] = {
    
    val g1 = graph.pregel(Array[(VertexId, (String, Double, String))](),3, EdgeDirection.Either)(
        (id, oldAttr, newAttr) => 
          if(newAttr.length == 0){//初始化信息合并方式
            //将声明值减1，然后在每个顶点上添加key是rec属性
            oldAttr.updated("life", ((oldAttr.apply("life").asInstanceOf[String]).toInt-1)+"").+("rec" -> Map[String, List[(VertexId, Double, String)]]())
          }else if(oldAttr.apply("type").asInstanceOf[String].equals("person") && oldAttr.apply("life").asInstanceOf[String].toInt == 2){//第一次迭代，圈子直接关系用户的邻居信息的聚合
        	  oldAttr.updated("life", ((oldAttr.apply("life").asInstanceOf[String]).toInt-1)+"").+("rec" -> newAttr)//用户顶点生命值减1，并将收到的信息放到属性map中
          }else if(oldAttr.apply("type").asInstanceOf[String].equals("circle") && oldAttr.apply("life").asInstanceOf[String].toInt == 2){//第二次迭代，圈子信息的聚合
            oldAttr.updated("life", ((oldAttr.apply("life").asInstanceOf[String]).toInt-1)+"").+("rec" -> newAttr)//将圈子顶点的生命值减一，最后将得到的信息保存到属性map中
          }else if(oldAttr.apply("life").asInstanceOf[String].toInt == 1 && oldAttr.apply("type").asInstanceOf[String].equals("person")){//第三次迭代，将圈子上的推荐信息聚合到圈子的直接关系用户上
          	val newAttrRdd = sc.parallelize(newAttr)//RDD[(VertexId, (String, Double, String))]-->RDD[(hash后id, (类型, score, 业务id))]
      			//对数据进行去重，分组，分组后数据格式是：RDD[(String, Iterable[(VertexId, (String, Double, String))])]-->RDD[(被推荐物品类型, Iterable[(VertexId, (被推荐物品类型, score, 被推荐物品业务id))])]
      			val filterData = newAttrRdd.map(x => (x._1, x._2)).reduceByKey((x, y) => (x._1, x._2 + y._2, x._3)).groupBy(x => x._2._1)
      			//对数据格式进行重新整理,整理后数据是：Map[String, List[(VertexId, Double, String)]]-->Map[type, List[(hash后id, score, 业务id)]]
      			//期望：格式是：Map[]
      			val dealData = filterData.map{x => 
      			(x._1, x._2.map(x => (x._1, x._2._2, x._2._3)).toList)  
          	}.collect().toMap
          	oldAttr.+("rec" -> dealData)//将得到的信息保存到map中
          }else{
            oldAttr
          },
         triplet => 
           if(triplet.srcAttr.apply("life").asInstanceOf[String].toInt == 1 && triplet.dstAttr.apply("life").asInstanceOf[String].toInt == 2 && triplet.dstAttr.apply("type").asInstanceOf[String].equals("circle")){//第二次迭代，开始发送消息到圈子顶点了
             //获取到圈子的一度用户和用户的下一级直接关系物品的亲密度score，格式是:Array[(VertexId, (String, Double))]，乘上圈子和直接用户的score作为圈子二度关系物品的得分
              //然后重新组成字符串发送到圈子节点上
              val attr = triplet.srcAttr.apply("rec").asInstanceOf[Array[(VertexId, (String, Double, String))]].map { x => 
                val score = x._2._2 * triplet.attr.toDouble 
                (x._1, (x._2._1, score, x._2._3))  
              }
              
              //因为基于圈子给用户推荐好友，圈子的直接关系用户被推荐给圈子的另一个用户的概率理论会远远大于，圈子的二度用户，所以这里将圈子的直接关系用户顶点添加到发送的消息中
              val userInfo: Array[(VertexId, (String, Double, String))] = Array((triplet.srcId, (triplet.srcAttr.apply("type").asInstanceOf[String], triplet.attr, triplet.srcAttr.apply("businessId").asInstanceOf[String])))
              //将圈子的直接关系用户顶点信息追加到attr上
              val circleInfo = attr.++:(userInfo)
          	  Iterator((triplet.dstId, circleInfo))
           }else if(triplet.dstAttr.apply("life").asInstanceOf[String].toInt == 1 && triplet.srcAttr.apply("life").asInstanceOf[String].toInt == 1 && triplet.dstAttr.apply("type").asInstanceOf[String].equals("circle")){//第三次迭代，将圈子上的推荐信息聚合到圈子直接相关的人上
             Iterator((triplet.srcId, triplet.dstAttr.apply("rec").asInstanceOf[Array[(VertexId, (String, Double, String))]]))
           }else if(triplet.srcAttr.apply("life").asInstanceOf[String].toInt == 2 && triplet.dstAttr.apply("life").asInstanceOf[String].toInt == 2){//初始化后第一次迭代
             Iterator((triplet.srcId, Array((triplet.dstId, (triplet.dstAttr.apply("type").asInstanceOf[String], triplet.attr, triplet.dstAttr.apply("businessId").asInstanceOf[String])))))
           }else{
             Iterator.empty
           },
        (a,b) => a.++:(b)
    )
    //进行去重,获取每个用户顶点对应的邻居节点id
    val neiborIdForPerson = g1.aggregateMessages[String](
    		triplet =>
    		triplet.sendToSrc(triplet.dstId+""),
    		(x, y) => x+"|"+y,
    		TripletFields.Src
    		).map(x => (x._1, (x._2.split("\\|").+:(x._1+"")).map { x => x.toLong }))
    val pointData = g1.vertices.filter(x => x._2.apply("type").asInstanceOf[String].equals("person"))
    .map{x => 
      var recData = Map[String, List[(VertexId, Double, String)]]()
      if(x._2.apply("rec").isInstanceOf[Map[String, List[(VertexId, Double, String)]]]){
        recData = x._2.apply("rec").asInstanceOf[Map[String, List[(VertexId, Double, String)]]]
      }else{
        recData = Map[String, List[(VertexId, Double, String)]]()
      }
      (x._1, (x._2.apply("businessId").asInstanceOf[String], recData))
    }
    //将推荐的结果中过滤掉用户顶点的直接关系顶点
    //返回的结果：RDD[(String, String, Map[String, List[(String, Double)]])]-->RDD[(命名空间名称, 用户的业务id, Map[推荐物品类型, List[(推荐物品业务id, score)]])]
    val recResultBasedCircle = pointData.join(neiborIdForPerson).map{x => 
      //x=>(VertexId, ((String, Map[String, List[(VertexId, Double, String)]]), Array[Long]))-->(用户hashid， ((用户业务id， Map[类型， List[(被推荐物品hashId, score, 被推荐物品业务id)]])))
      val filterData = x._2._1._2.map(y => (y._1, y._2.filter(z => !x._2._2.contains(z._1)).map(x => (x._3, x._2))))
      (x._2._1._1,filterData)
    }.collect
    
    //RDD[(VertexId, Map[String, List[(String, String)]])]
    recResultBasedCircle.foreach(x => println("基于圈子的推荐："+x._1+" 的推荐结果："+x._2.mkString(",")))
    recResultBasedCircle
  }
  
  /*
   * 计算user和一度好友之间的亲密度
   * edgeData[(Long, Long, Int, Long)]
   * 				srcId,dstId,communicateNum,time（time代表的是用户之间最近一次交互的时间和当前时间的时间差） 
   * comAvg:用户之间的平均交互次数；
   * 
   * 返回值：RDD[(long,long,Double)]-->(srcId,dstId,score)
   */
  def countCohesion(edgeData: RDD[(Long, Long, Int, Long)], comAvg: Double): RDD[(Long, Long, Double)] = {
    //计算亲密度
    val cohesion = edgeData.map { x =>
      var scoreCom: Double = 0
      var totalScore: Double = 0
      if (comAvg != 0) {
        scoreCom = (x._3 - comAvg) * 0.6 / comAvg
        //时间差的指数作为分母
        totalScore = scoreCom / math.pow(x._4 / 3600000, 1.5)
      } else {
        totalScore = -1.0
      } 
      (x._1, x._2, totalScore)
    }

    //cohesion.foreach { x => println(x._1+" 与 "+x._2+" 相似度是  "+x._3) }
    cohesion
  }

  /*
   * 标识不同物品id的工具方法
   */
  //Hashing方法
  def hashId(name: String, str: String) = {
    Hashing.md5().hashString(name+""+str).asLong()
  }

   
  /*
   * 基于一度好友推荐好友、物、圈子
   * 使用pregel，迭代两次，第一次dst顶点发送消息给src顶点，第二次dst顶点发送它收到的第一次迭代的消息给src顶点
   * vprog:
   * 		1.初始化：初始化信息是 Array[(VertexId, (String, Double, String))]()，vprog函数收到这个信息后，给图的每个顶点上添加life->2属性
   * 		2.第一次迭代收到的消息是 Array[(VertexId, (type, Double, 业务id))]((...))，将其以kv形式保存到map属性上，k是num1,value是数组；
   * 		3.第二次迭代收到的消息是Array[(VertexId, (type, Double, 业务id))]((...))，将其以kv形式保存到map属性上，k是num2,value是数组；
   * 			这个时候要对数组进行处理：包括对相同VertexId的数据进行分数累加去重和分组，最后的处理结果格式是：Map[String, List[(VertexId, Double, String)]]
   * 
   * sendMsg：
   * 		1.第一次迭代：发送 Array[(VertexId, (type, Double, 业务id))]((...))给src顶点
   * 		2.第二次迭代：限定只有收到第一次迭代消息的顶点才可以发送消息（发送消息的方向设置为IN），发送的消息是第一次迭代收到的消息；
   * 				第二次发送数据时候，要将第一次迭代收到的消息中的分数乘上当前triplet上的score作为最终的score
   * 
   * mergeMsg：
   * 		1.第一次迭代，收到的消息是Array[(VertexId, (type, Double, 业务id))]((...))，将这些消息使用.++合并为一个Array
   * 		2.第二次迭代，收到的消息是Array[(VertexId, (type, Double, 业务id))]((...))，将这些消息使用.++合并为一个Array
   * 两次迭代完成后，图的每个顶点上的属性中都有key是num2，value是二度关系顶点的一个属性，然后开始进行去重，去除掉属性中包含了用户直接关系顶点的信息
   * 
   * 返回值类型是：RDD[(String, Map[String, List[(String, Double)]])]
   */
  var count = 2
  def recBasedSimUser(graph: Graph[Map[String, Object], Double]): Array[(String, Map[String, List[(String, Double)]])] =  {
    val g2 = pregelDemo(graph)
    //进行去重,获取每个用户顶点对应的邻居节点id
    val neiborIdForPerson = g2.aggregateMessages[String](
    		triplet =>
    		triplet.sendToSrc(triplet.dstId+""),
    		(x, y) => x+"|"+y,
    		TripletFields.Src
    		).map(x => (x._1, (x._2.split("\\|").+:(x._1+"")).map { x => x.toLong }))
    val pointData = g2.vertices.filter(x => x._2.apply("type").asInstanceOf[String].equals("person"))
    .map(x => 
      try{
        (x._1, (x._2.apply("businessId").asInstanceOf[String], x._2.apply("rec").asInstanceOf[Map[String, List[(VertexId, Double, String)]]]))
      }catch{
        case ex: NoSuchElementException => (x._1, ("", Map[String, List[(VertexId, Double, String)]]()))
      }
    )
    //将推荐的结果中过滤掉用户顶点的直接关系顶点
    //返回的结果：RDD[(String, String, Map[String, List[(String, Double)]])]-->RDD[(命名空间名称, 用户的业务id, Map[推荐物品类型, List[(推荐物品业务id, score)]])]
    val recResultBasedCircle = pointData.join(neiborIdForPerson).map{x => 
      //x=>(VertexId, ((String, Map[String, List[(VertexId, Double, String)]]), Array[Long]))-->(用户hashid， ((用户业务id， Map[类型， List[(被推荐物品hashId, score, 被推荐物品业务id)]])))
      val filterData = x._2._1._2.map(y => (y._1, y._2.filter(z => !x._2._2.contains(z._1)).map(x => (x._3, x._2)).sortBy(x => x._2)))
      (x._2._1._1,filterData)
    }.collect()
    
    //RDD[(VertexId, Map[String, List[(String, String)]])]
    recResultBasedCircle.foreach(x => println("基于好友的推荐："+x._1+" 的推荐结果："+x._2.mkString(",")))
    recResultBasedCircle
  }
  
  //********************************************************************
   def pregelDemo(graph: Graph[Map[String, Object], Double]): Graph[Map[String, Object], Double] = {
    
    val two=2  //这里是二跳邻居 所以只需要定义为2即可 
    val newG=graph.pregel( Array[(VertexId, (String, Double, String))](), two, EdgeDirection.In)(vprog, sendMsg, addMaps)
    newG
    
  }   
  
  def vprog(vid:VertexId,oldAttr:Map[String, Object],newAttr:Array[(VertexId, (String, Double, String))])
  :Map[String, Object]={
    //每调用一次该方法，那么life生命值减1,初始值是3
    if(newAttr.size == 0){//初始化
    	oldAttr.updated("life", ((oldAttr.apply("life").asInstanceOf[String]).toInt-1)+"").+("rec" -> Map[String, List[(VertexId, Double, String)]]())
    }else if((oldAttr.apply("life").asInstanceOf[String]).toInt == 2){//第一次迭代,收到的信息格式：Array[(VertexId, (String, Double, String))]
      val attr = oldAttr.updated("life", ((oldAttr.apply("life").asInstanceOf[String]).toInt-1)+"")
      attr.+("num1" -> newAttr)
    }else if((oldAttr.apply("life").asInstanceOf[String]).toInt == 1){//第二次迭代,收到的信息格式：Array[(VertexId, (String, Double, String))]
      //这个时候要对数组进行处理：包括对相同VertexId的数据进行分数累加去重和分组,最终存储的格式是：Map[type, List[(VertexId, Double, 业务id)]]
      val dealData = sc.parallelize(newAttr).reduceByKey((x ,y) => (x._1, x._2+y._2, x._3)).groupBy(x => x._2._1).map(x => (x._1, (x._2.map(y => (y._1,y._2._2,y._2._3)).toList))).collect().toMap
      oldAttr.updated("rec" ,dealData)
    }else{
      oldAttr
    }
  }
  
  def sendMsg(triplet:EdgeTriplet[Map[String, Object], Double]): Iterator[(Long, Array[(VertexId, (String, Double, String))])] ={
	  //println("sendMsg生命值："+(triplet.srcAttr.apply("life").toString()).toInt)
	   //每次调用sendMsg方法，都对相应的dstId顶点上的属性6减一，用来表示这个顶点迭代的次数
    if((triplet.srcAttr.apply("life").asInstanceOf[String]).toInt == 2){//第一次迭代发送的数据格式：Array[(VertexId, (String, Double, String))]
    	Iterator((triplet.srcId, Array[(VertexId, (String, Double, String))]((triplet.dstId, (triplet.dstAttr.apply("type").asInstanceOf[String], triplet.attr, triplet.dstAttr.apply("businessId").asInstanceOf[String])))))
    }else{//第二次迭代,限定只有收到第一次迭代消息的顶点才可以发送消息（发送消息的方向设置为IN），发送的消息是第一次迭代收到的消息；
   				//第二次发送数据时候，要将第一次迭代收到的消息中的分数乘上当前triplet上的score作为最终的score
      val itera1 = triplet.dstAttr.apply("num1").asInstanceOf[Array[(VertexId, (String, Double, String))]]//取出每个dst顶点上的map中的“rec”键对应的value，类型是：Array[(String,String)],分别代表id和类别:score类型的数组
    	  val itera2 = itera1.map(x => (x._1, (x._2._1, x._2._2*triplet.attr, x._2._3)))
        Iterator((triplet.srcId, itera2)) 
    }
  }  
  
  def addMaps(data1: Array[(VertexId, (String, Double, String))], data2:  Array[(VertexId, (String, Double, String))]): Array[(VertexId, (String, Double, String))] ={
		data1.++:(data2)
  }
  
  /*
   * 将基于好友的推荐结果和基于圈子的推荐结果进行合并,并将合并好的数据放入到kafka中
   * 传来的数据格式：Array[(String, Map[String, List[(String, Double)]])]-->Array[(用户的业务id, Map[推荐物品类型, List[(推荐物品业务id, score)]])]
   * 返回的结果格式：Array[(String, String, Map[String, List[(String, Double)]])]-->Array[(命名空间名称, 用户的业务id, Map[推荐物品类型, List[(推荐物品业务id, score)]])]
   * 
   * 处理过程：
   * 1.首先将传来的基于好友的推荐结果和基于圈子的推荐结果合并为一个Array
   * 2.将相同用户id的推荐信息合并为一个map
   * 		(1).将两个推荐结果map先转化为list，然后合并到一起
   * 		(2).对list集合按照类型进行分组
   * 		(3).对分组后内容进行合并和去重
   * 				这里，经过之前的分组，同一个用户的相同类型的推荐结果分到了一组，并组成一个list,所以这里就需要对同一类型的推荐结果组成的list进行处理
   *				list2的数据格式是：Map[String, List[(String, List[(String, Double)])]]-->Map[类型, List[(类型, List[(被推荐物品的业务id, score)])]]
   * 				1.对分组后的由相同类型推荐内容组成的list的数据进行合并，就是将同一种类型的推荐结果(list)合并为一个list
   * 				2.因为合并后的内容是由多个可能存在重复id的被推荐物品组成的，所以这里需要对相同id的数据进行分数累加，并去重
   * 3.将命名空间加入到结果中
   * 4.将结果RDD转化为一个数组
   */  
  def combineResult(name:String, recBasedFriends: Array[(String, Map[String, List[(String, Double)]])], recBasedCircle: Array[(String, Map[String, List[(String, Double)]])]){
    //1.先将两个数组合到一起
    val arr1 = recBasedFriends.++:(recBasedCircle)
    //println("数据组合完成，开始对map进行合并")
    //2.将相同用户id的推荐信息(map)合并为一个map,返回结果是;RDD[(String, Map[String, List[(String, Double)]])]-->RDD[(用户业务id, Map[推荐物品类型, List[(推荐物品业务id, Double)]])]
    val arr2 = sc.parallelize(arr1).reduceByKey((x, y) => mapCombine(x, y))
    arr2.collect()
    //println("map合并完成，开始将命名空间加入到结果中")
    //3.将命名空间名称加入到结果中
    val arr3 = arr2.map(x => (name, x._1, x._2))
    //4.将结果RDD转化为一个数组
    val recResult = arr3.collect()
    recResult.foreach(x => println("命名空间："+x._1+" 下 "+x._2+"的推荐结果是："+x._3.mkString(",")))
  }
  
  /*
   * 参数代表同一个用户下的基于圈子的推荐信息和基于好友的推荐信息
   * Map[String, List[(String, Double)]]-->Map[type, List[(被推荐物品业务id, score)]]
   * 将两个map进行合并，条件是相同的用户id，对list进行合并，并对合并后的list进行分数累加和去重
   * 返回的是一个新的Map[String, List[(String, Double)]]
   */
  def mapCombine(data1: Map[String, List[(String, Double)]], data2: Map[String, List[(String, Double)]]): Map[String, List[(String, Double)]] = {
    val t1 = System.currentTimeMillis()
    //1.将两个推荐结果map先转化为list，然后合并到一起
    val list1 = data1.toList.++(data2.toList)//List[(String, List[(String, Double)])]
    //2.对list集合按照类型进行分组
    val list2 = list1.groupBy(x => x._1)
    //3.对分组后内容进行合并和去重
    //这里，经过之前的分组，同一个用户的相同类型的推荐结果分到了一组，并组成一个list,所以这里就需要对同一类型的推荐结果组成的list进行处理
    //list2的数据格式是：Map[String, List[(String, List[(String, Double)])]]-->Map[类型, List[(类型, List[(被推荐物品的业务id, score)])]]
    val result = list2.map{x => //x:(String, List[(String, List[(String, Double)])])
      //对分组后的由相同类型推荐内容组成的list的数据进行合并，就是将同一种类型的推荐结果(list)合并为一个list
      //返回值：List[(String, Double)]，代表当前类型下的推荐结果组成的list
      val comData = x._2.map(_._2).reduce((x, y) => x.++(y))
      //因为合并后的内容是由多个可能存在重复id的被推荐物品组成的，所以这里需要对相同id的数据进行分数累加，并去重
      val addScoreData = comData.groupBy(_._1).map(x => (x._1, x._2.map(_._2).reduce((x, y) => x + y))).toList
      (x._1, addScoreData)
    }
    
    val t2 = System.currentTimeMillis()  
    //println("合并结果用时："+(t2-t1))
    result
  }
}













