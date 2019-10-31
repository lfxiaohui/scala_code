package com.kuaibao

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.commons.lang3.math.NumberUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.functions.{collect_set, concat_ws, count}

import scala.collection.Map

object node2vec {

    Logger.getLogger("org").setLevel(Level.WARN)

    //编码空间
//    var space_size = 268435456
    //分桶区间

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf()
        val ymd = args(0)
        val ymd_sta = args(1)
        sparkConf.set("spark.driver.maxResultSize", "8G").set("spark.debug.maxToStringFields", "200")
        val spark = SparkSession.builder()
                .appName("FMRecall")
                .enableHiveSupport().config(sparkConf).getOrCreate()

        //        spark.sparkContext.hadoopConfiguration.set("mapred.output.compress", "false")
        //        sparkConf.set("spark.sql.parquet.compression.codec","gzip")

        val sc = spark.sparkContext
        import spark.implicits._

        //查询训练数据

        val coupon_ctr =
            s"""
               |SELECT device_id,
               |       concat_ws(",",collect_set(obj_coupon_id)) as coupon_list
               |FROM hds.sqkb_newapplog_coupon
               |WHERE device_id != ''
               |  AND device_id IS NOT NULL
               |  AND obj_item_id != ''
               |  AND obj_item_id IS NOT NULL
               |  AND obj_item_id <> '0'
               |  AND event_name=="coupon_click"
               |  AND ymd >= '20191001' group by device_id
           """.stripMargin

        val sim_coupon_ctr = spark.sql(coupon_ctr).toDF("device_id","coupon_list")

        sim_coupon_ctr.map{x=>
          val coupon_list = x.getAs[String]("coupon_list").split(",").combinations(2).toArray
            coupon_list.map(y=>(y.sorted.mkString(",")))
        }.flatMap(x=>x).toDF("pairwise").groupBy("pairwise").agg(count("pairwise").alias("num")).map{x=>
          val pairwise = x.getAs[String]("pairwise")
          val num = x.getAs[Long]("num")
            pairwise.split(",")(0)+" "+pairwise.split(",")(1)+" "+num.toString
        }.write.format("text").mode("overwrite").save("/user/xiaohui/node2vec_data/")


    }

}
