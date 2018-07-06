package study.graphx.algorithm

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vectors, Vector}

/*
 * SVD算法探究
 * 
 */
object SVDTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("triCounts").setMaster("local[3]")
		val sc = new SparkContext(conf)
    val edgePath = "E:\\spark\\Spark-GraphX\\data\\SVDTest\\a.mat"

    val data = sc.textFile(edgePath).map(_.split(" ").map (_.toDouble)).map { x => Vectors.dense(x) }
    //通过RDD[vectors]创建行矩阵
    val mat = new RowMatrix(data)
    val svd: SingularValueDecomposition[RowMatrix,Matrix] = mat.computeSVD(3,computeU = true)
    
    //通过访问svd对象的V、U和s成员分别拿到进行SVD分解后的右奇异矩阵、左奇异矩阵和奇异值向量
    
    val s: Vector = svd.s
    val u: RowMatrix = svd.U
    val v: Matrix = svd.V
    
    println(s)//[28.741265581939565,10.847941223452608,7.089519467626695,5.656901773857827,1.968086291967519E-7]
    println(u)//org.apache.spark.mllib.linalg.distributed.RowMatrix@50ac3dff
    println(v)
    //-0.32908987300830383  0.6309429972945555    0.16077051991193514   
    //-0.2208243332000108   -0.1315794105679425   -0.2368641953308101   
    //-0.35540818799208057  0.39958899365222394   -0.147099615168733    
    //-0.37221718676772064  0.2541945113699779    -0.25918656625268804  
    //-0.3499773046239524   -0.24670052066546988  -0.34607608172732196  
    //-0.21080978995485605  0.036424486072344636  0.7867152486535043    
    //-0.38111806017302313  -0.1925222521055529   -0.09403561250768909  
    //-0.32751631238613577  -0.3056795887065441   0.09922623079118417   
    //-0.3982876638452927   -0.40941282445850646  0.26805622896042314
    
    
  
  }
}