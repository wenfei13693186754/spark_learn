package model.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.reflect.runtime.universe

object SparkSql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    conf.setAppName("sql")
    
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc) 
    // 将普通rdd转为dataFrame
    import sqlContext.implicits._
    
    val df1 = sc.parallelize(List( (1,"张三",10),(2,"李四",20),(3,"王五", 20), (4,"赵六", 40))).toDF("id", "name", "deptid")
        
    val df2 = sc.parallelize(List( (10, "市场部"),(20, "教学部")  )).toDF("did", "dname")
    
    // 参数1 另一个df,   参数2 连接条件,   参数3 连接类型
    // inner 内链接,  left 做外连接 
    df1.join(df2, $"deptid" === $"did", "inner").show
    df1.join(df2, $"deptid" === $"did", "left").show
    
    // 可以将Column 跟其他值进行 + - * / 运算
    df1.select($"id" + 1).show
    
    val df3 = sc.parallelize(List( ("张三", Array(1,2,3))  )).toDF("name", "fn")
    // 根据表达式进行查询
    df3.selectExpr("fn[0]").show
    
    // 比如有下列json文件：
    // {"name":"张三","address":{"city":"北京","street":"海淀区东北旺"}}
    // {"name":"李四","address":{"city":"上海","street":"静安区区108号"}}
    val df4 = sqlContext.read.json("addr.json")
    df4.select("address.street", "address.city", "name").show
    
    df4.count // 返回记录数
    
    val row = df4.first()   
    row.getString(1) // 取得第一列的值，下标从1开始
    
    df4.collect
    df4.show(100) // 表示最多显示前100条记录
    df4.show      // 最多显示前20条记录
    
    // 当遇到shuffle 运算时，使用的partitions个数
    sqlContext.setConf("spark.sql.shuffle.partitions","2")
    
    
    // 如何关系型数据库获取数据，创建dataFrame对象
    val prop = new java.util.Properties
    prop.put("user", "数据库用户名")
    prop.put("password", "数据库密码")
    val df6 = sqlContext.read.jdbc("jdbc:mysql://数据库ip:3306/库名", "表名", prop)
    
    // 注意：通过jdbc创建需要数据库驱动包，并添加启动参数，例如：
    // bin/spark-shell --master=local --driver-class-path=mysql-connector-java-5.1.32.jar
    // bin/spark-submit ... --driver-class-path=mysql-connector-java-5.1.32.jar
    
    sc.textFile("README.md").flatMap { x => x.split(" ") }.toDF("word").registerTempTable("words")
      
    sqlContext.sql("select word, count(*) c from words group by word order by c desc limit 3").show
    
    // 19 appTypeCode, 23应用大类 ， 34 上行流量
    sc.textFile("/root/1.csv").map{
      x => ("a|" + x).split("\\|", -1)
    }.map { x => (x(19), x(23).toInt, x(34).toInt) }.toDF("appCode", "appType", "upBytes")
      .registerTempTable("source")
    
    // 要先过滤appCode不是103 的数据, 要根据各个appType的上行流量  
    val sql = "select appType, sum(upBytes) s from source where appCode = '103' group by appType order by s desc"
    sqlContext.sql(sql).show
    
    // 跟数据库建表不同，这张表就对应一个外部文件，不能通过sql对表中数据进行增删改
    // 如果删除了外部文件，那么这个表就不可用了
    // 表的定义信息，（metadata 表的元数据), 被存存储于 derby 数据库  
    val sql1 = """
                  CREATE TABLE people
                  USING org.apache.spark.sql.json
                  OPTIONS (
                    path "/root/people.json"
                  )
                  """
    sqlContext.sql(sql1)
    
    sqlContext.sql("cache table 表名") // 会把表的数据缓存至每个worker
    
    
  }
}