package com.kuaibao

import java.lang.ArrayIndexOutOfBoundsException
import java.io.FileNotFoundException
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.commons.lang3.math.NumberUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.Map

object FMRecall_v1 {

    Logger.getLogger("org").setLevel(Level.WARN)

    //编码空间
//    var space_size = 268435456
    //分桶区间
    var bin_scores = Array(0, 3, 3.9, 4, 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.9, 5)
    var bin_rate = Array(0.0, 10, 20, 30, 40, 50, 60, 70, 80, 90)
    var bin_price = Array(0.0, 5, 10, 15, 25, 50, 70, 150, 200, 250, 300)
    var bin_cm = Array(0.0, 2, 4, 6, 8, 10, 14, 20, 40)

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

        val repartition = 500
        val train_output_path = "/user/xiaohui/fm_recall_coupon_v1/"
        val feature = "S,Sc,Sd,Se,Sm"

        //查询训练数据
        var qur_train_label_feature =
            s"""
             select * from
               |(SELECT label, predictor_json,row_number() over (partition by trace_id, device_id, coupon_id order by label desc ) num
               | FROM alg.adm_sqkb_trainlog_hour
               | WHERE log_type = 'home_feed'
               | AND length(device_id) >= 14
               | AND length(coupon_id) >= 2
               | AND day <= '$ymd'
               | AND day >= '$ymd_sta') t
                where num=1
           """.stripMargin

        val coupon_ctr =
            s"""
               |SELECT obj_coupon_id,
               |       if(nvl(sum(if(event_name=="coupon_click",1,0))/sum(if(event_name=="coupon_view",1,0)),0.0001)=0,0.0001,nvl(sum(if(event_name=="coupon_click",1,0))/sum(if(event_name=="coupon_view",1,0)),0.0001)) as ctr
               |FROM hds.sqkb_newapplog_coupon
               |WHERE device_id != ''
               |  AND device_id IS NOT NULL
               |  AND obj_item_id != ''
               |  AND obj_item_id IS NOT NULL
               |  AND obj_item_id <> '0'
               |  AND ymd >= '$ymd_sta' group by obj_coupon_id
           """.stripMargin


        println("save file path: " + train_output_path)

        val qur_coupon_subcate =
              s"""
                 select coupon_id,subcate_id,month_sales
                 |from hds.coupon_product_v2
                 |where
                 |status != 0
                 |and product_type = 1
               """.stripMargin

            //查询商品和商品二级类目
        val coupon_subcate = spark.sql(qur_coupon_subcate).toDF("coupon_id","subcate_id","month_sales")


        val coupon_cvr = spark.sql(coupon_ctr).toDF("coupon_id","ctr")

        //生成编码数据2
        val train_df = spark.sql(qur_train_label_feature).map(x => (x.getAs[String]("label"), x.getAs[String]("predictor_json")))
                .rdd.coalesce(repartition).map(x => (if (x._1 == "0") "0" else "1", jsonStrToMap(x._2)))
                .map{x =>
                  val info= extractFeature(x._2, feature.split(","))
                    (info.split(",")(0).split(":")(0),info)
                }.toDF("coupon_id","hash_code")
                .dropDuplicates("coupon_id")



        val result = train_df.join(coupon_subcate,Seq("coupon_id")).select("coupon_id","hash_code","subcate_id","month_sales")
          .join(coupon_cvr,Seq("coupon_id")).select("coupon_id","hash_code","subcate_id","month_sales","ctr")
            .map{x=>
                x.getAs[String]("coupon_id")+"\t"+x.getAs[String]("subcate_id")+"\t"+x.getAs[String]("month_sales")+"\t"+x.getAs[String]("hash_code")+"\t"+x.getAs[Float]("ctr")
            }

        //保存编码数据
        result.write.format("text").mode("overwrite").save(train_output_path)

    }

    //哈希编码
    def myhash(str: String): String = {
        val seed = 131 // 31 131 1313 13131 131313 etc..
        val hash_space = 134217728
        var hash = 0
        for (i <- 0 until str.length()) {
            hash = hash * seed + str(i).toInt
        }
        hash = hash & 0x7FFFFFFF
        hash = hash % hash_space
        if(str.split(":").size >1){
            str.split(":")(1) + ":" + hash.toString
        }else{
            "-1" + ":" + hash.toString
        }


    }

    //提取特征
    def extractFeature(feaDic: Map[String, Any], feature: Array[String]): String = {
        val arr = feature.map(x => {
            var value = "-1"
            if (feaDic.contains(x)) {
                value = feaDic(x).toString.trim
            }
            if (Array("Mc", "Md", "Me").contains(x)) myhash(x + ":" + binning(value, bin_scores))
            else if (Array("Sl", "Sj").contains(x)) myhash(x + ":" + binning(value, bin_rate))
            else if (x == "Sf") myhash(x + ":" + binning(value, bin_price))
            else if (x == "Ss") myhash(x + ":" + binning(value, bin_cm))
            else myhash(x + ":" + value)
        })
        arr.mkString(",")
    }


    //Json String转换为Map
    def jsonStrToMap(jsonStr: String): Map[String, Any] = {
        implicit val formats = org.json4s.DefaultFormats
        parse(jsonStr).extract[Map[String, Any]]
    }

    //二分查找分桶区间
    def binarySearch(arr: Array[Double], l: Int, h: Int, searchNumber: Double): Int = {
        var high = h
        var low = l
        var mid = (low + high) / 2
        if (low >= mid) {
            low + 1
        } else if (arr(low) == searchNumber) {
            low + 1
        } else if (arr(low) < searchNumber && arr(mid) > searchNumber) {
            binarySearch(arr, low, mid, searchNumber)
        } else if (arr(mid) <= searchNumber && arr(high) > searchNumber) {
            binarySearch(arr, mid, high, searchNumber)
        } else if (arr(high) <= searchNumber) {
            high + 1
        } else -1
    }

    //分桶
    def binning(num: String, bins: Array[Double]): Int = {
        if (bins == null || bins.length == 0) throw new IllegalArgumentException("分桶列表出错...")
        var value = -1.0
        if (NumberUtils.isParsable(num)) {
            value = num.toDouble
        }
        binarySearch(bins, 0, bins.length - 1, value)
    }

    def backDay(date: String, days: Int): String = {
        val dateFormat = new SimpleDateFormat("yyyyMMdd")
        val startDate = dateFormat.parse(date)
        var cal = Calendar.getInstance()
        cal.setTime(startDate)
        cal.add(Calendar.DATE, -days);
        val newDate = dateFormat.format(cal.getTime())
        return newDate
    }

    def backHour(date: String, hours: Int): String = {
        val dateFormat = new SimpleDateFormat("yyyyMMddHH")
        val startDate = dateFormat.parse(date)
        var cal = Calendar.getInstance()
        cal.setTime(startDate)
        cal.add(Calendar.HOUR, -hours);
        val newDate = dateFormat.format(cal.getTime())
        return newDate
    }

    def getSomeString(x: Option[Any]) = x match {
        case Some(s) => if (s == null) "null" else s.toString
        case None => "None"
    }

    def getSomeInt(x: Option[Any]) = x match {
        case Some(s) => if (s == null) 0 else s.toString.toInt
        case None => 0
    }

    def getSomeFloat(x: Option[Any]) = x match {
        case Some(s) => if (s == null) 0.0f else s.toString.toFloat
        case None => 0.0f
    }
}
