package com.wdcloud.Utils

import org.joda.time.LocalDate
import java.util.Date
import java.text.DecimalFormat
import java.text.SimpleDateFormat


/**
 * 时间操作
 * 计算时间亲密度
 * @author Administrator
 *步骤：
 *	1.获取到每个一度好友和user最近一次交互的时间t1;
	2.用当前时间t0-t1,得到时间差t2；
	3. 将user和每个一度好友的时间差t2累加求平均时间差tAvg;
	4.tSohesion = (t2-tAvg)/tAvg
 */
object DataTime {
  def main(args: Array[String]): Unit = {
    val data = new LocalDate()
    println(data)
  }
  
  /*
   * 计算时间差
   */
  def getCoreTime(start_time:String,end_Time:String)={
    var df:SimpleDateFormat=new SimpleDateFormat("HH:mm:ss")
    var begin:Date=df.parse(start_time)
    var end:Date = df.parse(end_Time)
    var between:Long=(end.getTime()-begin.getTime())/1000//转化成秒
    var hour:Float=between.toFloat/3600
    var decf:DecimalFormat=new DecimalFormat("#.00")
    decf.format(hour)//格式化

  }
  
  /*
   * 计算今天时间
   */
 /* def getNowDate(): String = {
    var now: Data = new Data()
    var dataFormat: SimpleDataFormat = new SimpleDateFormat("yyyy-MM-dd")
    var nowTime = dataFormat.format(now)
  }*/
}