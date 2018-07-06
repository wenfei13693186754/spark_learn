package com.wdcloud.MLlib.recBasedLabel

object sortFunction {
  def main(args: Array[String]): Unit = {
    val list = List(("1",5.0), ("2",4.0), ("3",3.0), ("4",2.0), ("5",7.0),("6",6.0))
    println(msort((x:(String, Double) , y:(String, Double) ) => x._2<y._2)(list))
    println(reverse_pairs)
  }
  
    var reverse_pairs = 0//逆序数
    def msort[T](cmp:(T, T) => Boolean)(L:List[T]):List[T] = {
      //定义一个方法，这个方法有两个参数，一个是cmp:(T, T) => Boolean，它是一个自定义的谓语比较方法，用于比较T类型对象的大小
      //一个是L:List[T]:定义了一个集合
        def merge(xs:List[T], ys:List[T]):List[T]=(xs, ys)match{
            case(Nil, ys) => ys//Nil:The empty list.
            case(xs, Nil) => xs
            case(x::xs1, y::ys1) =>
                if(cmp(x, y))
                	y::merge(xs, ys1)
                else{
                  reverse_pairs += xs.length
                  x::merge(ys, xs1)//::=>Adds an element at the beginning of this list.
                } 
        }
        val n = L.length /2
        if(n == 0){
          return L
        }
        else{
          val(l1, l2) = L.splitAt(n)//splitAt:在给定位置将未知将列表拆分为两个
          merge(msort(cmp)(l1), msort(cmp)(l2))
        }
    }
   
}

