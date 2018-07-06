package redis

import redis.clients.jedis.Jedis

/**
 * 参考：http://www.csuldw.com/2016/06/26/2016-06-26-spark-and-redis-pipeline-exp/
 * 
 * 当数据量在十万级别的时候，采用这种逐行读取的方式没问题，但是到了千万级别就不行了，
 * 即使使用了spark的mapPartition也无法解决，所以这个时候需要使用BigDataHandle中
 * Redis提供的pipeline方法了
 *
 * 需要导入的包
 * 		redis.clients.jedis.Jedis
 * 
 * 逐行操作redis中的数据
 */
object SmallDataHandle {
  def main(args: Array[String]): Unit = {
    
    //建立redis连接
    val redisHost = "localhost"
    val redisPort = 8080
    val redisPassword = ""
    val redisClient = new Jedis(redisHost, redisPort)
    
    //如果redis有密码需要对密码进行验证
    redisClient.auth(redisPassword)
    
    //读取redis中的数据
    val keys = Array("k1","k2","k3","k4","k5")
    for(key <- keys){
      println(redisClient.get(key))
    }
    
    //可以通过redisClient对象来对redis进行操作....
  }
}