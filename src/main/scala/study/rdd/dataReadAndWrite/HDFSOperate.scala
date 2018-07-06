package study.rdd.dataReadAndWrite

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._
import org.json4s.jvalue2monadic
import org.json4s.string2JsonInput
import org.apache.hadoop.fs.Path

/**
 * scala对hdfs的操作
 */
object HDFSOperateExample {
  def main(args: Array[String]): Unit = {
    saveDataToHdfs
    readDataFromHdfs
    deleteFileMethod2
    deleteFileMethod3
    deleteFileMethod4
  }
  
  /**
   * 将rdd用不同方式保存到hdfs上
   */
  def saveDataToHdfs(){
    val conf = new SparkConf().setAppName("graphDemo").setMaster("local[*]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc: SparkContext = new SparkContext(conf) 
    
    val data = sc.parallelize(Array("Zara", "Nuha", "Ayan"))
    val path = "hdfs://192.168.6.84:9000/graph/edges"  
    //用于将RDD以对象的格式保存到hdfs文件系统中  
    data.saveAsObjectFile(path)
    //用于将RDD以文本文件的格式存储在hdfs文件系统中
    data.saveAsTextFile(path)
    //下边第二个参数是指定了压缩方式
    //data.saveAsTextFile(path, classOf[conf.hadoop.compression.lzo.LzopCodec])
  }
  
  //从hdfs中读取文件数据
  def readDataFromHdfs(){  
    val conf = new SparkConf().setAppName("graphDemo").setMaster("local[*]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc: SparkContext = new SparkContext(conf) 
    
    val hdfsRDD = sc.textFile("hdfs://192.168.6.84:9000/graph/edges", 5)
    sc.objectFile("", 5)
  }
    
  //通过设置可直接覆盖文件路径
  def deleteFileMethod2(){
    val conf = new SparkConf().setAppName("graphDemo").setMaster("local[*]")   
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
  } 
  
  //通过Hadoop方式删除已经存在的文件目录
  def deleteFileMethod3(){
    val path = new Path("hdfs://***")
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://**"), new org.apache.hadoop.conf.Configuration())
    if(hdfs.exists(path)){
      hdfs.delete(path, false)
    }
  } 
  
  //通过spark自带的hadoopconf方式删除
  def deleteFileMethod4(){
    val conf = new SparkConf().setAppName("graphDemo").setMaster("local[*]")   
    val sc = new SparkContext(conf)
    val hadoopconf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopconf)
    val path = new Path("hdfs://***")
    if(hdfs.exists(path)){
      //为了防止误删，禁止递归删除
      hdfs.delete(path, false)
    } 
  } 
}













