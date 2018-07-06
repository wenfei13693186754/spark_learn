package com.wdcloud.MLlib.TFIDF


// $example on$
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
// $example off$
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.avro.ipc.specific.Person


/**
 * 使用TF-IDF从文档中生成特征向量
 */
object TfIdfExample {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TfIdfExample").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 生成测试数据方式1,
    //注意，这里HashingTF使用的数据集DataSet只能是DataFrame
    val sentenceData = sqlContext.createDataFrame(Seq(
      (3, "中 国"),
      (2, "中 中 国 中 国"),
      (0, "I wish Java could use case classes"),
      (1, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    
    //创建分解器Tokenizer，用来将输入的字符串都转化为小写，然后按空格split
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    //使用分解器将数据进行转化
    val wordsData = tokenizer.transform(sentenceData)
    
    /*
     * [WrappedArray(hi, i, heard, about, spark),0]
       [WrappedArray(i, wish, java, could, use, case, classes),0]
       [WrappedArray(logistic, regression, models, are, neat),1]
     */
    wordsData.select("words","label").foreach(println)
    
    //创建hashingTF对象,这里如果不设置NumFeatures(20),那么默认的特征维度是2的18次方，也就是262144
    //当特征维度比较低的时候会存在潜在的哈希冲突，也就是不同的行特征可能会有相同的term
    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    //将输入的数据使用hashingTF转化为特征向量
    val featurizedData = hashingTF.transform(wordsData)
    
    /*
     *[(20,[5,6,9],[2.0,1.0,2.0]),0]
      [(20,[5,12,14,18],[1.0,2.0,1.0,1.0]),1]
			[(20,[3,5,12,14,18],[2.0,2.0,1.0,1.0,1.0]),0]
     */
    featurizedData.select("rawFeatures","label").foreach(println)
    
    //创建idf对象
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    //调用idf对象的fit方法，获得model，它代表语料库中的逆文档频率
    val idfModel = idf.fit(featurizedData)
    //对model调用transform(),用来将TF和IDF进行相乘,得到TF-IDF,这个唯一代表这篇文章的向量----->其底层就是先将idf进行广播，然后进行简单相乘
    val TFIDF = idfModel.transform(featurizedData)
    
    /*
     * [(20,[5,6,9],[0.0,0.6931471805599453,1.3862943611198906]),0]
       [(20,[3,5,12,14,18],[1.3862943611198906,0.0,0.28768207245178085,0.28768207245178085,0.28768207245178085]),0]
			 [(20,[5,12,14,18],[0.0,0.5753641449035617,0.28768207245178085,0.28768207245178085]),1]
     */
    TFIDF.select("features", "label").take(3).foreach(println)
    val vector = TFIDF.select("features", "label").map { x => x.getAs("label") }
    // $example off$
  }
}
// scalastyle:on println
