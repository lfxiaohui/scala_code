package com.kuaibao


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import com.kuaibao.NDCG

object Recall_off_evaluation {

  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.maxResultSize", "12G")
    sparkConf.set("spark.maxRemoteBlockSizeFetchToMem", "1G").set("spark.debug.maxToStringFields", "200")
    val spark = SparkSession.builder()
      .appName("ItemCF")
      .enableHiveSupport().config(sparkConf).getOrCreate()
    import spark.implicits._

    if (args.length != 3) {
      println("Args Errors!")
    }
    val ymd = args(0)
    val coupon_file_path = args(1)

    val coupon_ctr =
      s"""
         |SELECT obj_coupon_id,click,view,ctr FROM(
         |SELECT obj_coupon_id,
         |sum(if(event_name=="coupon_click",1,0)) as click,
         |sum(if(event_name=="coupon_view",1,0)) as view,
         |nvl(sum(if(event_name=="coupon_click",1,0))/sum(if(event_name=="coupon_view",1,0)),0.0) as ctr
         |FROM hds.sqkb_newapplog_coupon
         |WHERE device_id != ''
         |  AND device_id IS NOT NULL
         |  AND obj_item_id != ''
         |  AND obj_item_id IS NOT NULL
         |  AND obj_item_id <> '0'
         |  AND ymd >= '$ymd' group by obj_coupon_id ) a where click < view and click >0 and view > 10
           """.stripMargin


      val coupon_ctr_map = spark.sql(coupon_ctr).toDF("coupon_id","click","view","ctr").cache()
//    coupon_ctr_map.map(x=>x(0)+"\t"+x(1)+"\t"+x(2)+"\t"+x(3)).write.format("text").mode("overwrite").save("/user/xiaohui/itemcf_evaluation/")

    for(path <- coupon_file_path.split(",")){
//      val tmp_itemcf_result = "/user/algorithm/cf/tmp/reconstruct_itemcf_result_sorted_by_cate/"

      println(path+":")
      val result = spark.read.textFile(path)

      val result_coupon = result.map(_.split("\t")).map(x => x(1).split(",").zipWithIndex.map(y=>(x(0),y))).flatMap(x=>x).
          map(x=>(x._1,x._2._1,x._2._2)).toDF("main_coupon_id","sim_coupon","index")


      result_coupon.join(coupon_ctr_map,$"sim_coupon"===$"coupon_id").
        selectExpr("main_coupon_id","sim_coupon","index","cast(ctr as string) ctr").
        groupBy("main_coupon_id").
        agg(concat_ws(",",collect_set(concat_ws(":",col("sim_coupon"),col("ctr"),col("index")))).alias("sim_list")).map{x=>
        val coupon_id = x.getAs[String]("main_coupon_id")
        val sim_list = x.getAs[String]("sim_list").split(",").map(y=>y.split(":")).sortBy(x=>x(2).toInt).map(x=>x(0)+":"+x(1)).mkString(",")
        (coupon_id,NDCG.ndcg_k(sim_list,40))
      }.toDF("coupon_id","ndcg").describe("ndcg").take(10).foreach(println)


  }
  }

}

