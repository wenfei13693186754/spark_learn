package source

import org.apache.spark.Logging
import java.util.concurrent.ConcurrentHashMap
import org.apache.spark.util.Utils
import scala.collection.mutable.LinkedHashSet
import org.apache.spark.serializer.KryoSerializer
import scala.collection.JavaConverters._

class SparkConfDemo(loadDefaults: Boolean) extends Cloneable with Logging {

  def this() = this(true)

  private val settings = new ConcurrentHashMap[String, String]()

  def set(key: String, value: String): SparkConfDemo = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value")
    }
    settings.put(key, value)
    this
  }

  def getOption(key: String): Option[String] = {
    Option(settings.get(key)).orElse(SparkConfDemo.getDeprecatedConfig(key, this))
  }

  def contains(key: String): Boolean = settings.containsKey(key)
  
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }
  
  def registerKryoClasses(classes: Array[Class[_]]): SparkConfDemo = {
    val allClassNames = new LinkedHashSet[String]()
    allClassNames ++= get("", "").split(",").filter(!_.isEmpty())
    allClassNames ++= classes.map(_.getName)
    
    set("", allClassNames.mkString(","))
    set("", classOf[KryoSerializer].getName)
    this
  }
  
  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }
  
  override def clone: SparkConfDemo = {
    new SparkConfDemo(false).setAll(getAll)
  }
  
  def setAll(settings: Traversable[(String, String)]): SparkConfDemo = {
    settings.foreach{case (k, v) => set(k, v)}
    this
  }
  
  def getAll: Array[(String, String)] = {
    settings.entrySet().asScala.map{x=> (x.getKey, x.getValue)}.toArray
  }
}

private object SparkConfDemo extends Logging {
	def getDeprecatedConfig(key: String, conf: SparkConfDemo): Option[String] = {
		configsWithAlternatives.get(key).flatMap { alts => 
		  alts.collectFirst{case alt if conf.contains(alt.key) =>
		    val value = conf.get(alt.key)
		    if(alt.translation != null) alt.translation(value) else value
		  }  
		}
	}
  private val configsWithAlternatives = Map[String, Seq[AlternateConfig]](
    "spark.executor.userClassPathFirst" -> Seq(
      AlternateConfig("spark.files.userClassPathFirst", "1.3")),
    "spark.history.fs.update.interval" -> Seq(
      AlternateConfig("spark.history.fs.update.interval.seconds", "1.4")))
}

private case class AlternateConfig(
  key: String,
  version: String,
  translation: String => String = null)



