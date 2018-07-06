package com.wdcloud.MLlib.recFriends

import org.apache.spark.mllib.recommendation.Rating
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable._
import org.apache.hadoop.hbase.client.Put

/**
 * 从HBase中读取数据
 * 向HBase保存数据
 */
class hbaseUtilsLinux {
  
  /**
   * 从HBase中读取用户信息  
   */
  def bathRec(ids: Array[String]): ArrayBuffer[(String,Array[String])] = {
    //获取数据
		val conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum","namenode:2181,datanode1:2181,datanode2:2181,datanode3:2181");
    conf.set("hbase.defaults.for.version.skip", "true")
		val table = new HTable(conf,"UIC_TEST")
    val usersArray = collection.mutable.ArrayBuffer[(String,Array[String])]()
    for(id <- ids){
    	val get = new Get(Bytes.toBytes(id))
    	val result = table.get(get);
    	val bs = result.getValue(Bytes.toBytes("YH"),Bytes.toBytes("FRIENDS"));
    	val str = Bytes.toString(bs).split(",")
 			usersArray.+=:(id,str)      
 			usersArray.foreach(x=>println(x._1+"用户，它的好友是："+x._2.mkString(",")))
 			println("OK")
    }
		table.close();         
    usersArray  
  }
     
  /**   
   * 向HBase中写入用户信息
   *
   */
  def saveRecResult(tableName: String, rowKey:String,familyName: String, columnName: String,data: Array[Int]): Unit = {
    //write("UIC_TEST", "002", "YH", "FRIENDS")
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "namenode,datanode1,datanode2,datanode3");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.set("hbase.defaults.for.version.skip", "true")
    val table = new HTable(conf, Bytes.toBytes(tableName));
    val put = new Put(Bytes.toBytes(rowKey))  
    put.addColumn(familyName.getBytes(), columnName.getBytes(), Bytes.toBytes(data.mkString(",")));
    table.put(put)
    table.close();
  }
}