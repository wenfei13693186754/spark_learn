package redis

import redis.clients.jedis.Jedis
import redis.clients.jedis.Response

/**
 * 参考：http://www.csuldw.com/2016/06/26/2016-06-26-spark-and-redis-pipeline-exp/
 * 
 * 当数据量在十万级别的时候，采用这种逐行读取的方式没问题，但是到了千万级别就不行了，
 * 即使使用了spark的mapPartition也无法解决，所以这个时候需要使用BigDataHandle中
 * Redis提供的pipeline方法了
 *
 * 需要导入的包
 * 		redis.clients.jedis.Jedis
 * 		redis.clients.jedis.Pipeline
 * 		redis.clients.jedis.Response
 * 逐行操作redis中的数据
 */
object BigDataHandle {
  def main(args: Array[String]): Unit = {
    
    //简化版读取redis数据
    handleRedis1()
    
    //加强版读取redis数据
    handleRedis2()
  }
  //使用pipline读取数据(简化版)
  //因为redis.clients.jedis.Jedis的pipelined下的get方法获取的是一个
  //Response[String]类型的返回值，所以上面定义了一个临时变量Map[String, Response[String]]
  //类型的tempRedisRes，key是String类型，value是Response[String]类型，
  //用于保存pp.get(key)的返回值。当for循环执行完之后，使用sync同步即可。
  //这样便实现了Redis的Pipeline功能。
  def handleRedis1() {
    //建立redis连接
    val redisHost = "localhost"
    val redisPort = 8080
    val redisPassword = ""
    val redisClient = new Jedis(redisHost, redisPort)
    redisClient.auth(redisPassword)

    var tempRedisRes = Map[String, Response[String]]()
    val keys = Array("k1", "k2", "k3", "k4", "k5")
    val pp = redisClient.pipelined()
    for (key <- keys) {
      tempRedisRes ++= Map(key -> pp.get(key))
    }
    pp.sync()
  }

  //使用pipline读取数据(加强版)
  //为了防止连接Redis时的意外失败，我们需要设置一个尝试次数，确保数据一定程度上的正确性。
  //因此，在上面代码外面增加一层连接逻辑
  def handleRedis2() {
    //建立redis连接
    val redisHost = "localhost"
    val redisPort = 8080
    val redisPassword = ""
    val redisClient = new Jedis(redisHost, redisPort)
    redisClient.auth(redisPassword)

    var tempRedisRes = Map[String, Response[String]]()
    val keys = Array("k1", "k2", "k3", "k4", "k5")
    var tryTimes = 2
    var flag = false
    while (tryTimes > 0 & !flag) {
      try {
        val pp = redisClient.pipelined()
        for (key <- keys) {
          tempRedisRes ++= Map(key -> pp.get(key))
        }
        pp.sync()
        flag = true
      } catch {
        case e: Exception => {
          flag = false
          println("Redis_TimeOut" + e)
          tryTimes = tryTimes - 1
        }
      } finally {
        redisClient.disconnect()
      }
    }
  }
}



