package com.wdcloud.MLlib.KMeans

// scalastyle:off println
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
// $example on$
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StructField, StructType}
// $example off$

// $example off$

/**
 * An example demonstrating a LDA of ML pipeline.
 * Run with
 * {{{  
 * bin/run-example ml.LDAExample
 * }}}
 */
object LDAExample {
 // final val FEATURES_COL = "features"

  def main(args: Array[String]): Unit = {

    val input = "E:\\Spark-MLlib\\data\\dataBasedLabel\\sample_lda_data.txt"
    // Creates a Spark context and a SQL context
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // $example on$
    // Loads data
    val rowRDD = sc.textFile(input).filter(_.nonEmpty)
      .map(_.split(" ").map(_.toDouble)).map(Vectors.dense).map(Row(_))
    val schema = StructType(Array(StructField("features", new VectorUDT, false)))
    val dataset = sqlContext.createDataFrame(rowRDD, schema)

    // Trains a LDA model
    val lda = new LDA()
      .setK(10)
      .setMaxIter(10)
      .setFeaturesCol("FEATURES_COL")
    val model = lda.fit(dataset)
    val transformed = model.transform(dataset)

    //语料库中这篇log的下边界是。。。
    val ll = model.logLikelihood(dataset)
    //混合model的上边界是。。。
    val lp = model.logPerplexity(dataset)   

    // describeTopics
    val topics = model.describeTopics(3)

    // Shows the result
    topics.show(false)
    transformed.show(false)

    // $example off$
    sc.stop()
  }
}