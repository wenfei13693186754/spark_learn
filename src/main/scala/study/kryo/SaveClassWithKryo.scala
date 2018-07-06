package study.kryo

import java.io._
import java.util.zip.Deflater
import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}
import com.esotericsoftware.kryo.serializers.DeflateSerializer
import com.twitter.chill.{AllScalaRegistrar, EmptyScalaKryoInstantiator}
import org.apache.hadoop.io.compress.DeflateCodec
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import scala.reflect.ClassTag
import com.esotericsoftware.kryo.io.{Input => KryoInput}
import com.esotericsoftware.kryo.io.{Output => KryoOutput}
import org.apache.hadoop.io.compress.DeflateCodec
import org.apache.spark.rdd.RDD.rddToSequenceFileRDDFunctions

case class Person(name: String, age: Int)

/**
 * 使用kryo方式序列化方式，序列化对象并输出
 */
object SaveClassWithKryo {    
  def main(args: Array[String]): Unit = {
    val sc =  new SparkContext("local[2]", "test")
    val rdd = sc.parallelize(0 to 10000000).map(i => Person(s"X_$i", i))
    rdd.count //0.9s
    val jserPath = "hdfs://localhost:9000/user/xf/ser/person.jser"
    rdd.saveAsObjectFile(jserPath) //7.2s
    val rdd2: RDD[Person] = sc.objectFile(jserPath)
    rdd2.take(3)  
    rdd2.count //10.5s input:660.5M  
  
    val jserCompressPath =  "hdfs://localhost:9000/user/xf/ser/person_c.jser"
    KryoFile.saveAsObjectFile(rdd, jserCompressPath)
    val rdd4 = KryoFile.objectFile[Person](sc, jserCompressPath)
    rdd4.take(3)
    rdd4.count()
  
    val kryoPath = "hdfs://localhost:9000/user/xf/ser/person.kryo"
    KryoFile.saveAsKryoObjectFile(rdd, kryoPath) //group(10): 104.496724s,group(100):18.163698 s, not group: 1016.758094, in partition,group(100):6s, compress: 119.457758s, compress by DeflateCodec: 16s
  
    val rdd3 = KryoFile.objectKryoFile[Person](sc, kryoPath)
    rdd3.take(10).foreach(println)
    println(rdd3.count()) //group(10):634.154736 s  input:326.3 MB,group(100):72.697525 s,input 203.9 MB,inpartition,group(100): 77.284727 s,input 203.9 MB. mapPatition: 1.859009 s,203.9 MB, compress by DeflateCodec: 1.8s,26M
  
  }
}

/**
 * 使员工Kryo范式序列化对象，并输出
 */
object KryoFile {

  def toBytes(o: Person): Array[Byte] = {
    val kryo = (new EmptyScalaKryoInstantiator).newKryo()
    val clazz: Class[Person] = classOf[Person]
    val deflateSerializer = new DeflateSerializer(kryo.getDefaultSerializer(clazz))
    deflateSerializer.setCompressionLevel(Deflater.BEST_COMPRESSION)
    kryo.register(clazz, deflateSerializer)
    val bao = new ByteArrayOutputStream()
    val output = new KryoOutput(1024*1024)
    output.setOutputStream(bao)
    kryo.writeClassAndObject(output, o)
    output.close()
    // We are ignoring key field of sequence file
    bao.toByteArray
  }

  def toObject(bytes:  Array[Byte] ): Person = {
    val kryo = (new EmptyScalaKryoInstantiator).newKryo()
    //val clazz: Class[Person] = classOf[Person]
    //val deflateSerializer = new DeflateSerializer(kryo.getDefaultSerializer(clazz))
    //deflateSerializer.setCompressionLevel(Deflater.BEST_COMPRESSION)
    //kryo.register(clazz, deflateSerializer)
    new AllScalaRegistrar().apply(kryo)
    val input = new KryoInput()
    input.setBuffer(bytes)
    val data = kryo.readClassAndObject(input)
    val dataObject = data.asInstanceOf[Person]
    dataObject
  }


  /*
   * Used to write as Object file using kryo serialization
   */
  def saveAsKryoObjectFile[T: ClassTag](rdd: RDD[T], path: String) {

    rdd.mapPartitions(iter => {

      val kryo = (new EmptyScalaKryoInstantiator).newKryo()
      //val clazz: Class[Person] = classOf[Person]
      //val deflateSerializer = new DeflateSerializer(kryo.getDefaultSerializer(clazz))
      //deflateSerializer.setCompressionLevel(Deflater.BEST_COMPRESSION)
      //kryo.register(clazz, deflateSerializer)
      new AllScalaRegistrar().apply(kryo)
      iter.grouped(100).map(it => {
        val bao = new ByteArrayOutputStream()
        val output = new KryoOutput(1024*1024)
        output.setOutputStream(bao)
        kryo.writeClassAndObject(output, it.toArray)
        output.close()
        // We are ignoring key field of sequence file
        val byteWritable = new BytesWritable(bao.toByteArray)
        (NullWritable.get(), byteWritable)
      })
    }).saveAsSequenceFile(path, Some(classOf[DeflateCodec]))
    //, Some(classOf[DeflateCodec])
  }

  /*
   * Method to read from object file which is saved kryo format.
   */
  def objectKryoFile[T](sc: SparkContext, path: String, minPartitions: Int = 2)(implicit ct: ClassTag[T]) = {

    val kryoSerializer = new KryoSerializer(sc.getConf)

    sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], minPartitions).mapPartitions(iter => {
      //val kryo = (new EmptyScalaKryoInstantiator).newKryo()
      val kryo = kryoSerializer.newKryo()
      //val clazz: Class[Person] = classOf[Person]
      //val deflateSerializer = new DeflateSerializer(kryo.getDefaultSerializer(clazz))
      //deflateSerializer.setCompressionLevel(Deflater.BEST_COMPRESSION)
      //kryo.register(clazz, deflateSerializer)
      //new AllScalaRegistrar().apply(kryo)
      iter.flatMap(x => {
        val input = new KryoInput()
        input.setBuffer(x._2.getBytes)
        val data = kryo.readClassAndObject(input)
        val dataObject = data.asInstanceOf[Array[T]]
        dataObject
      })
    })

  }



  def saveAsObjectFile[T: ClassTag](rdd: RDD[T],path: String) {
    rdd.mapPartitions(iter => iter.grouped(100).map(_.toArray))
      .map(x => (NullWritable.get(), new BytesWritable(serialize(x))))
      .saveAsSequenceFile(path, Some(classOf[DeflateCodec]))
  }

  def objectFile[T: ClassTag](sc: SparkContext,
                              path: String,
                              minPartitions: Int = 2
                               ): RDD[T] = {
    sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], minPartitions)
      .flatMap(x => deserialize[Array[T]](x._2.getBytes, sc.getClass.getClassLoader))
  }

  /** Serialize an object using Java serialization */
  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }

  /** Deserialize an object using Java serialization */
  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[T]
  }
  def deserialize[T](bytes: Array[Byte], loader: ClassLoader): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis) {
      override def resolveClass(desc: ObjectStreamClass) =
        Class.forName(desc.getName, false, loader)
    }
    ois.readObject.asInstanceOf[T]
  }

}

