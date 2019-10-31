package com.kuaibao

//添加淘宝足迹数据
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object item2vec {

  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.maxResultSize", "12G")
    sparkConf.set("spark.maxRemoteBlockSizeFetchToMem", "1G").set("spark.debug.maxToStringFields", "200")
    val spark = SparkSession.builder()
      .appName("item2vec_train_data")
      .enableHiveSupport().config(sparkConf).getOrCreate()
    import spark.implicits._

//    val ymd = args(0)

    val qur_item_user =
      s"""
         |SELECT
         |    device_id,
         |    NVL(concat_ws(" ", collect_list(obj_coupon_id)), "-1") AS click
         |FROM
         |    (
         |        SELECT
         |            device_id,concat_ws(":",obj_coupon_id,obj_subcate_id) as obj_coupon_id,rank
         |        FROM
         |        (
         |            SELECT device_id,
         |                obj_coupon_id,
         |                obj_subcate_id,
         |                row_number() over(partition BY device_id ORDER BY obj_subcate_id DESC) AS rank
         |            FROM hds.sqkb_newapplog_coupon
         |                WHERE event_type="click"
         |                AND event_name ="coupon_click"
         |                AND obj_cate_id3 != ""
         |                AND obj_coupon_id != ""
         |                AND obj_cate_id3 is not NULL
         |                AND obj_coupon_id is not NULL
         |                AND device_id != "000000000000000"
         |                AND length(device_id) >= 14
         |                AND length(obj_coupon_id) >= 2
         |                AND ymd <= '20191023'
         |                AND ymd >= '20190923'
         |                AND cur_page!='h5_free' and cur_module!='home_free_list'
         |                and cur_page not in ('h5_subsidy','h5_subsidy_v2')
         |                group by device_id,obj_coupon_id,obj_subcate_id
         |        )  a1
         |        JOIN (select coupon_id from hds.coupon_product_v2 where status=1) a2
         |        ON a1.obj_coupon_id=a2.coupon_id
         |    ) a3
         |WHERE rank <= 100
         |GROUP BY device_id
           """.stripMargin

    spark.sql(qur_item_user).toDF("device_id","coupon_id").map{x=>
      val device_id = x.getAs[String]("device_id")
      val coupon_id = x.getAs[String]("coupon_id").split(" ").sortBy(_.split(":")(1)).mkString(" ")
      coupon_id
    }.filter(_.split(" ").size>=5).write.format("text").mode("overwrite").save("/user/xiaohui/item2vec_train_data/")


  }
}

