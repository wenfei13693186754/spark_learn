package com.wdcloud.spark.demo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * spark中的各种连接操作
 */
object joinOperate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("demo")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
     /**
      * id      name
      * 1       zhangsan
      * 2       lisi
      * 3       wangwu
      */
    val idName = sc.parallelize(Array((1, "zhangsan"), (2, "lisi"), (3, "wangwu")))
    
    /**
      * id      age
      * 1       30
      * 2       29
      * 4       21
      */
    val idAge = sc.parallelize(Array((1, 30), (2, 29), (4, 21)))
    /***************************************RDD*****************************************/
    println("*****************************RDD*************************************************")
    println("\n内关联(inner join) \n")
    //内关联(inner join)
    //只保留两边id相等的部分
    /**
     * (1,(zhangsan,30))
     * (2(lisi,29))
     */
    idName.join(idAge).collect().foreach(println)
    
    
    println("\n左外关联(left out join)\n")
    //左外关联(left out join)
    //以左边数据为标准，左边数据一律保留
    //右边分三种情况：
    //    一.左边的id，右边有，则合并数据：(1,(zhangsan,Some(30)))
    //    二.左边的id，右边没有，则右边为空；(3,(wangwu,None))
    //    三.右边的id，左边没有，则不保留；如右边有id为4的行，但结果中并未保留
    /**
     * (1,(zhangsan,Some(30)))
     * (2,(lisi,Some(29)))
     * (3,(wangwu,None)) 
     */
    idName.leftOuterJoin(idAge).collect().foreach(println)
    
    println("\n右外关联(right outer join)\n")
    //右外关联(right outer join)
    //以右边数据为准，右边数据一律保留
    //左边分三种情况：
    //     一：右边的id，左边有，则合并数据；(1,(Some(zhangsan),30))
    //     二：右边的id，左边没有，则不保留；右边有id为4的行，但结果中并未保留
    //     三：右边的id，左边没有，则左边为空；(4,(None,21))
    /**
      * (1,(Some(zhangsan),30))
      * (2,(Some(lisi),29))
      * (4,(None,21))
      */
    idName.rightOuterJoin(idAge).collect().foreach(println)
    
    println("\n全外关联(full outer join)\n")
    //两边数据都保留
    /**
     *  (4,(None,Some(21)))
        (1,(Some(zhangsan),Some(30)))
        (2,(Some(lisi),Some(29)))
        (3,(Some(wangwu),None))
     */
    idName.fullOuterJoin(idAge).collect().foreach(println)
    
    
    /************************cogroup与join*******************************************/   
    //当出现相同Key时，join会出现笛卡尔积，而cogroup处理方式不同
        /**
      * id      name
      * 1       zhangsan
      * 2       lisi
      * 3       wangwu
      */
    val idNameForCo = sc.parallelize(Array((1, "zhangsan"), (2, "lisi"), (3, "wangwu")))

    /**
      * id      age
      * 1       30
      * 2       29
      * 4       21
      */
    val idAgeForCo = sc.parallelize(Array((1, 30), (2, 29), (4, 21)))

    println("\ncogroup\n")

    /**
      * (1,(CompactBuffer(zhangsan),CompactBuffer(30)))
      * (2,(CompactBuffer(lisi),CompactBuffer(29)))
      * (3,(CompactBuffer(wangwu),CompactBuffer()))
      * (4,(CompactBuffer(),CompactBuffer(21)))
      */
    idNameForCo.cogroup(idAgeForCo).collect().foreach(println)

    println("\njoin\n")
    // fullOuterJoin于cogroup的结果类似, 只是数据结构不一样
    /**
      * (1,(Some(zhangsan),Some(30)))
      * (2,(Some(lisi),Some(29)))
      * (3,(Some(wangwu),None))
      * (4,(None,Some(21)))
      */
    idNameForCo.fullOuterJoin(idAgeForCo).collect().foreach(println)

    /**
      * id      score
      * 1       100
      * 2       90
      * 2       95
      */
    val idScore = sc.parallelize(Array((1, 100), (2, 90), (2, 95)))

    println("\ncogroup, 出现相同id时\n")

    /**
      * (1,(CompactBuffer(zhangsan),CompactBuffer(100)))
      * (2,(CompactBuffer(lisi),CompactBuffer(90, 95)))
      * (3,(CompactBuffer(wangwu),CompactBuffer()))
      */
    idNameForCo.cogroup(idScore).collect().foreach(println)

    println("\njoin, 出现相同id时\n")

    /**
      * (1,(Some(zhangsan),Some(100)))
      * (2,(Some(lisi),Some(90)))
      * (2,(Some(lisi),Some(95)))
      * (3,(Some(wangwu),None))
      */
    idNameForCo.fullOuterJoin(idScore).collect().foreach(println)
  }

}











