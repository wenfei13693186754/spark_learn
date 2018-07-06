package model.streaming.outputOperation

import org.apache.commons.pool2.impl.GenericObjectPool
import java.sql.Connection
import org.apache.commons.pool2.PooledObject
import java.sql.DriverManager
import org.apache.commons.pool2.BasePooledObjectFactory
import org.apache.commons.pool2.impl.DefaultPooledObject

/**
 * 借助org.apache.commons.pool2来实现ConnectionPool
 */  
object ConnectionPool {
  private val pool = new GenericObjectPool[Connection](new MysqlConnectionFactory("jdbc:mysql://localhost:3306/mysql", "root", "root", "com.mysql.jdbc.Driver"))
  
  def getConnection(): Connection ={
    pool.borrowObject()
  }

  def returnConnection(conn: Connection): Unit ={
    pool.returnObject(conn)
  }
}

class MysqlConnectionFactory(url: String, userName: String, password: String, className: String) extends BasePooledObjectFactory[Connection]{
  override def create(): Connection = {
    Class.forName(className)
    DriverManager.getConnection(url, userName, password)
  }

  override def wrap(conn: Connection): PooledObject[Connection] = new DefaultPooledObject[Connection](conn)

  override def validateObject(pObj: PooledObject[Connection]) = !pObj.getObject.isClosed

  override def destroyObject(pObj: PooledObject[Connection]) =  pObj.getObject.close()
}