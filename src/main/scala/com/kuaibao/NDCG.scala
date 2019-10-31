package com.kuaibao

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count

object NDCG {

    Logger.getLogger("org").setLevel(Level.WARN)

  def ndcg_k(coupon_list:String,k:Int):Double = {
    val coupon_ctr = coupon_list.split(",").map(x=>x.split(":")(1).toDouble).take(k)
    val coupon_ctr_sort = coupon_ctr.sorted.reverse
    println(coupon_ctr.mkString(","))
    println(coupon_ctr_sort.mkString(","))
    var dcg_sum:Double = 0.0
    var idcg_sum:Double = 0.0
    for(index <- (1 to coupon_ctr.length)){
      dcg_sum += (math.pow(2,coupon_ctr(index-1))-1)*(1/math.log(index+1))
      idcg_sum += (math.pow(2,coupon_ctr_sort(index-1))-1)*(1/math.log(index+1))
    }
    return  dcg_sum/idcg_sum
  }

}
