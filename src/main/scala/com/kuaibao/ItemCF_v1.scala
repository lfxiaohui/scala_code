package com.kuaibao

//添加淘宝足迹数据
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ItemCF_v1 {

  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.maxResultSize", "12G")
    sparkConf.set("spark.maxRemoteBlockSizeFetchToMem", "1G")
    val spark = SparkSession.builder()
      .appName("ItemCF_v1")
      .enableHiveSupport().config(sparkConf).getOrCreate()
    import spark.implicits._

    val ymd = args(0)
    val coupon_file_path = args(1)

    val qur_item_user =
      s"""
             select obj_item_id,device_id
         |  from hds.sqkb_newapplog_coupon
         |  where event_name="coupon_click"
         |        and device_id != ''
         |        and device_id is not null
         |        and obj_item_id != ''
         |        and obj_item_id is not null
         |        and obj_item_id <> '0'
         |        and ymd >= '$ymd'
         | union all
         |	select itemid as obj_item_id,device_id from
         			   |	(select device_id,itemid from hds.spider_user_coupon_action where and type = 'footprint' and ymd>='$ymd') a join
         			   |	(select item_id from hds.coupon_product_v2 where status != 0) b on a.itemid=b.item_id
           """.stripMargin

    //查询点击数据
    val item_user = spark.sql(qur_item_user)
      .map(x => (x.getAs[String]("obj_item_id"),x.getAs[String]("device_id")))
      .toDF("itemId","userId")
    //查询点击商品量在2和2000之间的用户
    val click_cnt = item_user.groupBy("userId").agg(count($"itemId").alias("cnt"))
      .filter("(cnt >= 2) and (cnt <= 2000)")
      .select("userId").distinct()
    //过滤点商品击量在2和2000之间的用户
    val filtered_item_user = item_user.distinct.join(click_cnt,"userId")
    println("item_user count: " + item_user.count())
    //计算每个商品的点击用户数量，过滤掉点击量小于5的用户
    val numUsersPerItem = filtered_item_user.groupBy("itemId").agg(count($"userId").alias("nor"))
      .select("itemId", "nor")
      .filter("nor >= 5")
    println("numRatersPerItem: " + numUsersPerItem.count)
    //关联商品用户和商品点击量
    val itemWithSize = item_user.join(numUsersPerItem, "itemId")
      .select("itemId", "userId", "nor")
    println("itemWithSize: " + itemWithSize.count)

    val qur_coupon_subcate =
      s"""
         select item_id,subcate_id
         |from hds.coupon_product_v2
         |where
         |status != 0
         |and product_type = 1
       """.stripMargin

    //查询商品和商品二级类目
    val coupon_subcate = spark.sql(qur_coupon_subcate)
    println("qur_coupon_subcate count: " + coupon_subcate.count())
    //关联商品、用户、二级类目、点击量数据
    val join = itemWithSize.join(coupon_subcate, $"itemId" === $"item_id").distinct()
    println("join count: " + join.count())
    //根据用户和二级类目聚合商品
    val joined = join.groupBy("userId", "subcate_id")
      .agg(concat_ws(",", collect_list(concat_ws(":", $"itemId", $"nor"))).alias("items"))
      .map(x => (x.getAs[String]("items"))).rdd
    println("joined count: " + joined.count())
    //计算相同用户和二级类目下所有的共现商品和点击量
    val combined = joined.map(x => (x.split(",").distinct.combinations(2).toArray))
      .flatMap(x => x.map(y => (y(0), y(1))))
    val multi_combined = combined.map(x => (x._2,x._1)).union(combined)
      .map(x => (x._1.split(":"), x._2.split(":")))
      .map(x => (x._1(0), x._1(1), x._2(0), x._2(1)))
      .toDF("item1", "nor1", "item2", "nor2")
    println("combined count: " + combined.count())
    //计算商品的共现次数
    val grouped = multi_combined.groupBy("item1", "item2")
      .agg(first("nor1").alias("nor1"),
        first("nor2").alias("nor2"),
        count("nor1").alias("size"))
      .select("item1", "item2", "size", "nor1", "nor2")

    println("grouped count: " + grouped.count())

    // 计算商品余弦相似度
    val similarities = grouped.map(row => {
      val cor = row.getAs[Long](2)
      val nor1 = row.getAs[String](3).toInt
      val nor2 = row.getAs[String](4).toInt
      val corr = cosSim(cor, nor1, nor2)
      (row.getString(0), row.getString(1), corr)
    }).toDF("itemId_01", "itemId_02", "corr")

    val qur_item_info =
      s"""
         |select item_id, coupon_id, shop_id
         |from hds.coupon_product_v2
         |where
         |is_recommend != 2
         |and product_type = 1
         |and ticket_id != 0
         |and status != 0
           """.stripMargin

    val product_is_del =
      s"""
         |select coupon_id, is_del
         |from hds.coupon_product_profile
           """.stripMargin

    val shop_is_del =
      s"""
         |select shop_id, is_del
         |from hds.coupon_shop
           """.stripMargin
    //查询商品是否被删除
    val product_deled = spark.sql(product_is_del).toDF("coupon", "produc_is_del")
    //查询商品店铺是否被删除
    val shop_deled = spark.sql(shop_is_del).toDF("shop", "shop_is_del")
    //过滤召回商品中不符合条件的商品
    val item_map = spark.sql(qur_item_info).
      join(product_deled, $"coupon_id" === $"coupon", "left_outer").
      join(shop_deled, $"shop_id" === $"shop", "left_outer").
      filter("(produc_is_del != 1) and (shop_is_del != 1)").
      select("item_id", "coupon_id")

    println("item_map: " + item_map.count)

    val qur_main_coupon =
      s"""
         |select item_id, coupon_id
         |from hds.coupon_product_v2
         |where
         |status != 0
           """.stripMargin
    val main_coupon = spark.sql(qur_main_coupon)
    println("main_coupon: " + main_coupon.count)
    //聚合每个商品的召回商品
    val filted_sim = similarities.join(item_map, $"itemId_02" === $"item_id")
      .select($"itemId_01", $"coupon_id", $"corr")
      .toDF("itemId_01", "sim_coupon", "corr")
      .join(main_coupon, $"itemId_01" === $"item_id")
      .select($"coupon_id", $"sim_coupon", $"corr")
      .toDF("main_coupon", "sim_coupon", "corr")
      .distinct()
    println("filted_sim: " + filted_sim.count)

    val qur_price =
      s"""
         |select coupon_id, raw_price, zk_price ,month_sales
         |from hds.coupon_product_v2
         |where
         |status != 0
         |and product_type = 1
           """.stripMargin
    val main_price = spark.sql(qur_price)
    println("main_price: " + main_price.count)
    //查询商品的原价和折扣价
    val sim_price = spark.sql(qur_price).toDF("coupon_id", "sim_raw_price", "sim_zk_price","sim_month_sales")
    println("sim_price: " + sim_price.count)
    //关联商品的价格特征
    val tmp_result = filted_sim.join(main_price, $"main_coupon" === $"coupon_id")
      .select("main_coupon", "sim_coupon", "corr", "zk_price")
      .join(sim_price, $"sim_coupon" === $"coupon_id")
      .select("main_coupon", "sim_coupon", "corr", "zk_price", "sim_raw_price", "sim_zk_price","sim_month_sales")
      .filter("sim_month_sales > 10")
      .groupBy("main_coupon")
      .agg(concat_ws(",", collect_list(concat_ws(":", $"sim_coupon", $"corr", $"zk_price", $"sim_raw_price", $"sim_zk_price",$"sim_month_sales"))).alias("simList"))
      .select("main_coupon", "simList")
      .map(x => (x.getAs[Int]("main_coupon").toString, x.getAs[String]("simList")))
    //把商品和推荐商品价格和相似度存hdfs
    val tmp_itemcf_result = "/user/xiaohui/tmp_itemcf_result"
//    tmp_result.map(x => x._1 + "\t" + x._2).toDF
//      .write.format("text").
//      mode("overwrite").
//      save(tmp_itemcf_result)
    //按照排序规则对相似商品重新排序
    val result = spark.read.textFile(tmp_itemcf_result)
      .map(_.split("\t")).map(x => (x(0),rank_sim(x(1)))).filter(_._2.size > 0)
      .map(x => x._1 + "\t" + x._2).toDF
    println("result: " + result.count)
    result.write.format("text").mode("overwrite").save(coupon_file_path)

    val item_cf_data  = spark.read.textFile("/user/algorithm/cf/tmp/reconstruct_itemcf_result_sorted_by_cate/")
    val item_cf_data_v1 = spark.read.textFile(coupon_file_path)

    val item_cf = item_cf_data.map{x=>
      val item = x.split("\t")(0)
      val item_set = x.split("\t")(1).split(",")
      val item_size = item_set.size
      (item,item_set,item_size)
    }.toDF("item","item_set","size")

    val item_cf_v1 = item_cf_data_v1.map{x=>
      val item = x.split("\t")(0)
      val item_set = x.split("\t")(1).split(",")
      val item_size = item_set.size
      (item,item_set,item_size)
    }.toDF("item","item_set","size")

    val item_cf_alldata = item_cf.join(item_cf_v1,Seq("item"),"right_outer").toDF("item","item_set1","size1","item_set2","size2").
      na.fill(Map("size1"->0))

    item_cf_alldata.map{x=>
      val item = x.getAs[String]("item")
      val size1 = x.getAs[Int]("size1")
      val size2 = x.getAs[Int]("size2")
      val item_set1 = x.getAs[Seq[String]]("item_set1")
      val diff = if(item_set1 == null){
        x.getAs[Seq[String]]("item_set2").take(200).mkString(",")+"\t"+"0"
      }
      else {
        x.getAs[Seq[String]]("item_set2").take(200).diff(item_set1.take(200)).mkString(",")+"\t"+"1"
      }
      item+"\t"+diff
    }.write.format("text").mode("overwrite").save("/user/xiaohui/item_cf_extend_result_data")


  }




  //计算商品间的余弦相似度
  def cosSim(cor: Long, nor1: Int, nor2: Int): Float = {
    cor.toFloat / math.sqrt(nor1 * nor2).toFloat
  }
  //按照排序规则重新对相似商品排序
  def rank_sim(str: String): String = {
    val sorted_by_corr = str.split(",").map(_.split(":")).sortBy(_(1).toFloat).reverse
    val m_price = sorted_by_corr.filter(x => ((x(3).toFloat / x(4).toFloat) > 1.04))
    val filtered_coupons = m_price.filter(x => {
      var price = x(2).toFloat
      var filter_price = price
      if (price >= 100f) {
        filter_price = price * 1.4f
      }
      else if ((price < 100f) && (price >= 50f)) {
        filter_price = price * 1.5f
      }
      else {
        filter_price = price * 1.6f
      }
      x(4).toFloat < filter_price
    }).map(x => x(0))
    (filtered_coupons ++ (m_price.map(_ (0)).diff(filtered_coupons))).mkString(",")
  }
}

