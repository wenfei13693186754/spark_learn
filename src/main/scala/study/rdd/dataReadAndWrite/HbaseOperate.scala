package study.rdd.dataReadAndWrite

import java.util.ArrayList

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 对hbase表的增删改查
 */
object HbaseOperate {
  def main(args: Array[String]): Unit = {
    //deleteTable()
    //createTable()
    putDataToTable()
    //getDataFromTable()
    //getRowsDataFromTable(Iterable[String]("0001~1499324117041~16ae7033000000ac"), "INFO", "IP")
    //readDataByHadoopApi()
  }
  //删除表
  def deleteTable() {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "namenode,datanode1,datanode2,datanode3");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    val admin = new HBaseAdmin(conf)
    val table_name = "JANUARY:TEST"
    if (admin.tableExists(table_name)) {
      admin.disableTable(table_name)
      admin.deleteTable(table_name)
    }
    println(table_name + "  表删除成功")
  }

  //创建表
  def createTable() {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "namenode,datanode1,datanode2,datanode3");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    val admin = new HBaseAdmin(conf)
    val table_name = "JANUARY:TEST"
    val htd = new HTableDescriptor(table_name)
    val hcd = new HColumnDescriptor("INFO")
    //add  column family to table
    htd.addFamily(hcd)
    admin.createTable(htd)
    println("表创建成功")
  }

  //向表中插入数据
  def putDataToTable() {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "namenode,datanode1,datanode2,datanode3");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    val table_name = "JANUARY:TEST"
    //put data to HBase table
    val table = new HTable(conf, table_name)
    val databytes = Bytes.toBytes("INFO")
    for (c <- 31 to 40) {
      val row = Bytes.toBytes("row" + c.toString)
      val p1 = new Put(row)
      p1.add(databytes, Bytes.toBytes(c.toString), Bytes.toBytes("value" + c.toString))
      p1.add(databytes, Bytes.toBytes(c.toString), System.currentTimeMillis(), Bytes.toBytes("value" + c.toString))
      table.put(p1)
    }
    println("插入数据成功")
  }

  //从表中获取数据
  def getDataFromTable() {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "zk1.cs,zk2.cs,zk3.cs");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    val table_name = "PTYHZX:T_SYS_LOGINRECORD"
    val rowkey = "0001~1499324117041~16ae7033000000ac"
    //put data to HBase table
    val table = new HTable(conf, table_name)
    val g = new Get(Bytes.toBytes(rowkey))
    val result = table.get(g) //这里获取的是一个result对象
    val userIp = Bytes.toString(result.getValue("INFO".getBytes, "IP".getBytes))
    println("Get:" + userIp)
  }

  /**
   * @Description:一次请求获取多行数据，它允许用户快速高效的从远处服务器获取相关的或者完全随机的多行数据
   * 				get(List<T>)要么返回和给定列表大小相同的result列表，要么抛出异常
   * @param rowkeys rowkey集合
   * @param family
   * @param column
   * @return Result[]: Result 集合
   */
  def getRowsDataFromTable(rowkeys: Iterable[String], family: String, columns: String*): Array[Result] = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "zk1.cs,zk2.cs,zk3.cs");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    val table_name = "PTYHZX:T_SYS_LOGINRECORD"
    val htable = new HTable(conf, table_name)
    val gets: java.util.ArrayList[Get] = new ArrayList[Get]()
    var results: Array[Result] = null
    val rowkey = rowkeys.iterator
    while (rowkey.hasNext) {
      val rk = rowkey.next()
      val g = new Get(rk.getBytes)
      for (colu <- columns) {
        g.addColumn(family.getBytes, colu.getBytes)
      }
      gets.add(g)
    }
    results = htable.get(gets)
    results.foreach { x => println(Bytes.toString(x.getValue("INFO".getBytes, "IP".getBytes))) }
    results
  }

  //search table
  def searchTable() {
    val table_name = "JANUARY:TEST"
    val config = HBaseConfiguration.create
    val sc = new SparkContext("local", "HBaseTest",
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_TEST_JAR")))
    config.set(TableInputFormat.INPUT_TABLE, table_name)
  }

  //使用Hadoop的api来操作***********************************************************
  /*
   * 读取hbase数据
   * 
   * 由于org.apache.hadoop.hbase.mapreduce.TableInputFormat类的实现，Spark可以通过Hadoop输入格式(
				sc.newAPIHadoopRDD(config, classOf[TableInputFormat],
  	      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
  	      classOf[org.apache.hadoop.hbase.client.Result])
		)访问Hbase。这个输入格式会返回键值对数据，其中键的类型是org.apache.hadoop.hbase.io.ImmutableBytesWritable,
		而值的类型为org.apache.hadoop.hbase.client.Result.Result类包含多种根据列获取值的方法，在其API文档
		(https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Result.html)中有所描述。
	

		TableInputFormat包含多个可以用来优化对HBase的读取选项，比如将扫描限制到一部分列中，以及限制扫描的时间范围。可以在
		TableInputFormat的API文档(https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Result.html#rawCells--)
		中找到这些选项，并在HBaseConfiguration中设置它们，然后再将它传给Spark.
		
		###Result类包含多种根据列获取值的方法
		###TableInputFormat包含多个可以用来优化对HBase的读取选项
		
   * newAPIHadoopRDD是用来读取其它Hadoop输入格式数据的
   * 它的接收一个路径以及三个类，如果有需要设定额外的Hadoop配置属性，也可以传入一个conf对象
   * 		它的三个类：
   * 				1.第一个类是“格式”类，代表输入的格式；
   * 				2.第二个则是键的类；
   * 				3.第三个类是值的类。
   * (因为我们可以通过Hadoop输入格式访问HBase,这个格式返回的键值对的数据中键和值的类型就是我们下边的类型)
   * 
   */
  def readDataByHadoopApi() {
    val table_name = "JANUARY:TEST"
    val config = HBaseConfiguration.create
    config.set("hbase.zookeeper.quorum", "namenode,datanode1,datanode2,datanode3");
    config.set("hbase.zookeeper.property.clientPort", "2181");
    config.set(TableInputFormat.SCAN_CACHEBLOCKS, "false") //指定扫描出的数据是不是进行缓存，false代表不缓存
    config.set(TableInputFormat.SCAN_BATCHSIZE, "10000") //指定每次扫描返回的数据量
    config.set(TableInputFormat.SCAN_COLUMN_FAMILY, "INFO") //指定扫描的列族|    

    val sc = new SparkContext("local", "HBaseTest",
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_TEST_JAR")))
    config.set(TableInputFormat.INPUT_TABLE, table_name)

    val hbaseRDD = sc.newAPIHadoopRDD(config,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = hbaseRDD.count()
    val confInfo = hbaseRDD.map { //这里是按行键读取的，也就是下边获得的key是一个一个的行健，不是一个行健组成的数组   行健：用户业务id_物品业务id
      x =>
        val result = x._2
        val row = result.rawCells()
        row.map { cell => (Bytes.toString(cell.getQualifier), Bytes.toString(cell.getValue)) }

    }.first()
  }

  //向hbase中写入数据
  def writeConf() {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "namenode,datanode1,datanode2,datanode3");
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
    val table = new HTable(hbaseConf, Bytes.toBytes("JANUARY:T_USER_CONF"));
    table.setAutoFlush(false, true) //开启缓存区
    table.setWriteBufferSize(100000)
    val admin = new HBaseAdmin(hbaseConf)
    //判断表是否存在，不存在则创建
    if (!admin.isTableAvailable("")) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(""))
      val hcd = new HColumnDescriptor("INFO")
      //add  column family to table  
      tableDesc.addFamily(hcd)
      admin.createTable(tableDesc)

    }

    val put = new Put(("user.business.id" + "_" + "user.scene.id").getBytes()) //为指定行创建一个Put操作，指定行健是用户业务id
    put.addColumn("INFO".getBytes(), "USERID".getBytes(), Bytes.toBytes("")); //写入一个cell数据
    put.addColumn("INFO".getBytes(), "namespace".getBytes(), Bytes.toBytes("")); //写入另一个cell数据
    table.put(put)
    table.flushCommits()
    table.close();
    admin.close()
  }

  //**********************************************************************************************************
  //使用JonConf来向hbase中写入数据

  /**
   * 使用JonConf对象来向hbase中保存数据，性能比直接使用spark提供的hadoopAPI向hbase中保存数据要好很多
   * JobConf对象是用来描述Hadoop mapreduce程序的主要接口，mapreduce的运行就是按照jobconf的描述进行的
   */
  def saveRDDToHbaseWithJobConf() {
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置  
    conf.set("hbase.zookeeper.quorum", "slave1,slave2,slave3")
    //设置zookeeper连接端口，默认2181  
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    val tablename = "account"

    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！  
    val jobConf = new JobConf(conf)

    //jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    val indataRDD = sc.makeRDD(Array("1,jack,15", "2,Lily,16", "3,mike,16"))

    val rdd = indataRDD.map(_.split(',')).map { arr =>
      {
        /*一个Put对象就是一行记录，在构造方法中指定主键  
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换  
       * Put.add方法接收三个参数：列族，列名，数据  
       */
        val put = new Put(Bytes.toBytes(arr(0).toInt))
        put.add(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes(arr(1)))
        put.add(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(arr(2).toInt))
        //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset  
        (new ImmutableBytesWritable, put)
      }
    }

    rdd.saveAsHadoopDataset(jobConf)

    sc.stop()
  }

  def saveRDDToHbaseWithJob() {
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val tablename = "account"

    sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "slave1,slave2,slave3")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    val job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val indataRDD = sc.makeRDD(Array("1,jack,15", "2,Lily,16", "3,mike,16"))
    val rdd = indataRDD.map(_.split(',')).map { arr =>
      {
        val put = new Put(Bytes.toBytes(arr(0)))
        put.add(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes(arr(1)))
        put.add(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(arr(2).toInt))
        (new ImmutableBytesWritable, put)
      }
    }

    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())
  }
}   
