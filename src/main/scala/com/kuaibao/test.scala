//package com.kuaibao
//
//import java.text.SimpleDateFormat
//import java.util.Calendar
//
//import org.apache.commons.lang3.math.NumberUtils
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession
//import org.json4s._
//import org.json4s.jackson.JsonMethods._
//import org.apache.spark.sql.functions._
//
//import scala.collection.Map
//
//object FMRecall_v1 {
//  def main(args: Array[String]): Unit = {
//    print("fff")
//  }
//}
//
//    Logger.getLogger("org").setLevel(Level.WARN)
//
//    //编码空间
////    var space_size = 268435456
//    //分桶区间
//    var bin_scores = Array(0, 3, 3.9, 4, 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.9, 5)
//    var bin_rate = Array(0.0, 10, 20, 30, 40, 50, 60, 70, 80, 90)
//    var bin_price = Array(0.0, 5, 10, 15, 25, 50, 70, 150, 200, 250, 300)
//    var bin_cm = Array(0.0, 2, 4, 6, 8, 10, 14, 20, 40)
//
//    def main(args: Array[String]): Unit = {
//        val sparkConf = new SparkConf()
//        val ymd = args(0)
//        val ymd_sta = args(1)
//        sparkConf.set("spark.driver.maxResultSize", "8G").set("spark.debug.maxToStringFields", "200")
//        val spark = SparkSession.builder()
//          .appName("FMRecall")
//          .enableHiveSupport().config(sparkConf).getOrCreate()
//
//        //        spark.sparkContext.hadoopConfiguration.set("mapred.output.compress", "false")
//        //        sparkConf.set("spark.sql.parquet.compression.codec","gzip")
//
//        val sc = spark.sparkContext
//        import spark.implicits._
//
//
//        val df1 =spark.read.json("/user/algorithm/cf/click_rec_spark_valid/*")
//
////         spark.read.textFile("").toDF("data").selectExpr("cast(data as String)")
//        val df2 = df1.limit(100)
//        df2.take(10).foreach(println)
//
//        import scala.util.parsing.json.JSON
//
//
//        implicit val matchError = org.apache.spark.sql.Encoders.kryo[Row]
//
//        def jsonStrToMap(jsonStr: String): Map[String, Any] = {
//            implicit val formats = org.json4s.DefaultFormats
//            parse(jsonStr).extract[Map[String, Any]]
//        }
//
//        df2.map{x=>
//          val json_str = x.getAs[String]("data").split("\t")(1)
//            implicit val formats = org.json4s.DefaultFormats
//            parse(json_str).extract[Map[String, Any]]
//    }.take(10).foreach(println)
//
//
//        df2.map{x=>
//          val json_str = x.getAs[String]("data").split("\t")(1)
//            JSON.parseFull(json_str)
//        }.take(10).foreach(println)
//
//        df2.map(x=>x.getInt(1)).take(10).foreach(println)
//
//        df2.map(x=>JSON.parseFull(x.getString(0).split("\t")(1))).take(10).foreach(println)
//
//
//        val df3 = df2.map(x=>x.getString(0).split("\t")(1)).toDF("data").selectExpr("cast(data as string) as data").cache()
//
//        df3.map(x=>JSON.parseFull(x.getString(1))).take(10).foreach(println)
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//    }
//}
