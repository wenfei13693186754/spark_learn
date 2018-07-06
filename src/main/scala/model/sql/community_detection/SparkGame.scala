package com.wdcloud.statistics

import scala.util.Try

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by hadoop on 2018/2/23.
  */
object SparkGame {
  def main(args: Array[String]) {

    if (args.length < 1) {
      System.err.println("============请在提交时添加参数: 1,2,3,4分别代表四个题目================")
      System.exit(1)
    }

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val buckets = try {
      sc.getConf.get("spark.cores.max").toInt * 3
    } catch {
      case e: Exception => 200
    }
    sqlContext.sql(s"set spark.sql.shuffle.partitions=$buckets")

    try {
      args(0) match {
        case "1" => top3Cell(sqlContext)
        case "2" => searchHot(sqlContext)
//        case "3" => hotNews(sqlContext)
        case "4" => popularWebSite(sqlContext)
        case _ =>
          System.err.println("=========参数错啦=========")
          System.exit(1)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sc.stop()
    }
  }

  // 第一题 各时段Top3小区
  private def top3Cell(sqlContext: HiveContext): Unit = {
    def sql =
      """
         select ci,usercount,date,hour,minute5*5 as minute
         from (
          select ci,date,hour,minute5,usercount,row_number() over (partition by hour,minute5 order by usercount desc) as rn
          from(
            select date,count(distinct imsi) as usercount,ci,hour,minute5
            from (
              select reportdate as date,reporthour as hour,cast(minute(clttime)/5 as int) as minute5,ci,imsi
              from fact_ipp_flux_limit
              where reportdate='2016-05-28' and reporthour in (14,15,16) and length(ci)=15
            ) a
            group by hour,minute5,ci,date
          ) b
         ) c where rn<=3
      """
    val temp = sqlContext.sql(sql).rdd.persist
    temp.collect.foreach(println)
  }

  // 第二题 搜索引擎关键词排名
  private def searchHot(sqlContext: HiveContext): Unit = {
    def sql =
      """
  select url from fact_ipp_flux_limit
      """
    val baiduReg = """(?<=&word=).*(?=(&|$))""".r
    val shenmaReg = """(?<=s\?q=).*(?=(&|$))""".r
    val sogouReg = """(?<=keyword=).*(?=&)""".r
    val q360Reg = """(?<=index.php\?q=).*(?=&)""".r

    val searchKW = sqlContext.sql(sql).rdd.map(row => row.getString(0)).filter(_.length > 1)
      .map { url =>
        val baidu = baiduReg findFirstIn url
        val shenma = shenmaReg findFirstIn url
        val sogou = sogouReg findFirstIn url
        val q360 = q360Reg findFirstIn url
        val list = List(baidu, shenma, sogou, q360).filter(_ != None)
        if (list.isEmpty)
          ""
        else
          Try {
            list.head.getOrElse("").split("&").head
          }.getOrElse("")
      }.filter(_.length > 0)
      .map { x => Try(java.net.URLDecoder.decode(x, "utf-8")).getOrElse("") }
      .filter(_.length > 0)
      .map((_, 1)).reduceByKey(_ + _)
      .sortBy(_._2, ascending = false).persist

    searchKW.take(150).foreach(println)
  }

  // 第三题 新闻热度
//  private def hotNews(sqlContext: HiveContext): Unit = {
//    def sql =
//      """
//  select imsi,url,reporthour from fact_ipp_flux_limit
//      """
//    val newsWebs = List("info.3g.qq.com", "kb.qq.com", "sina.cn", "toutiao.com", "yidianzixun", "ifeng.com", "sohu.com", "3g.163.com", "xinhuanet", "people.com", "myzaker", "weibo.com", "news")
//    val hotNews = sqlContext.sql(sql).rdd.map(row => (row.getString(0), row.getString(1), row.getInt(2))).filter(_._2.length > 1)
//      .map { urlInfo => if (newsWebs.exists(urlInfo._2.contains(_))) URLInfo(urlInfo._1, "1", urlInfo._3) else URLInfo(urlInfo._1, "0", urlInfo._3) }
//    hotNews.toDF.registerTempTable("newsurl")
//
//    def sql2 =
//      """select sum(p) as hotrate,reporthour
//                   from (
//                     select count(case when url=1 then 1 else null end)/count(*) as p,imsi,reporthour
//                     from newsurl
//                     group by imsi,reporthour
//                   ) as userrate
//                   group by reporthour"""
//
//    sqlContext.sql(sql2).show()
//  }

  // 第四题
  private def popularWebSite(sqlContext: HiveContext): Unit = {
    def sql =
      """
    select url from fact_ipp_flux_limit
      """

    val popularRDD = sqlContext.sql(sql).rdd
      .map(row => row.getString(0))
      .filter(_.length > 1)
      .map(x => filterDomin(x))
      .filter(_.length > 1)
      .map(x => x.split("\\.").init.last + "." + x.split("\\.").last)
      .map((_, 1)).reduceByKey(_ + _)
      .sortBy(_._2, ascending = false).persist

    //popularRDD.saveAsTextFile("/odpp/spaces/user07_space/files/popularwebsite")
    popularRDD.take(150).foreach(println)

  }


  private def filterDomin(str: String): String = {
    str.split("/", -1).dropRight(1).find(x => findTopDomain(x)).getOrElse("")
  }

  private def findTopDomain(x: String): Boolean = {
    val domain = List(".cn", ".com", ".net", ".org", ".gov", ".edu")
    domain.exists(x.contains(_))
  }
}

case class URLInfo(imsi: String, url: String, reporthour: Int)

