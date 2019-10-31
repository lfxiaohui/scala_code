
package com.kuaibao

//import com.kuaibao.SwingModel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_set, concat_ws, count}

/**
  * @author jinfeng
  * @version 1.0
  */
object Swing extends Serializable{

  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.shuffle.consolidateFiles","true")
      .set("spark.driver.maxResultSize", "12G")
      .set("spark.maxRemoteBlockSizeFetchToMem", "1G")
      .set("spark.sql.crossJoin.enabled","true")
      .set("spark.sql.shuffle.partitions","600")
    val spark = SparkSession.builder().config(conf=conf).getOrCreate()
//    val sc = spark.sparkContext
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

//    val qur_item_user =
//      s"""
//          select device_id,num_iid
//          from hdw.order_model
//          where device_id != ''
//            and device_id is not null
//            and num_iid != ''
//            and num_iid is not null
//            and num_iid <> '0'
//            and ymd >= '20190922'
//           """.stripMargin

    val qur_item_user =
      s"""
             select device_id,num_iid
         |  from hdw.order_model
         |  where device_id != ''
         |        and device_id is not null
         |        and num_iid != ''
         |        and num_iid is not null
         |        and num_iid <> '0'
         |        and ymd >= '20190916'
         | union all
         |   select device_id,itemid as num_iid from
         |	(select device_id,itemid from hds.spider_user_coupon_action where ymd>='20190916' and type="order") a join
         |	(select item_id from hds.coupon_product_v2 where status != 0) b on a.itemid=b.item_id
           """.stripMargin

    var ratings_init = spark.sql(qur_item_user).toDF("user_id","item_id").cache()

    val indexer = new StringIndexer().setInputCol("item_id").setOutputCol("item_id_int").fit(ratings_init)
    val indexed = indexer.transform(ratings_init)
    val ratings = indexed.selectExpr("user_id","item_id", "cast(item_id_int as int) item_id_int")
      .toDF("user_id","item_id","item_id_int")


//    println("ratings")

//    println("ratings" + ratings.take(10).foreach(println))
//    println("初始日志条数:"+ratings.count())

    //   stage-3
    val groupUsers = ratings.groupBy("user_id").
      agg(concat_ws(",",collect_set("item_id")).alias("item_set"),concat_ws(",",collect_set("item_id_int")).alias("item_set_int"),count("item_id").alias("count")).
      select("item_set","item_set_int","count")
      .toDF("item_set","item_set_int","count")
      .filter("count > 1")
      .repartition(600)
      .drop("count")
//
//    println("用户点击商品大于5小于500的人数:"+groupUsers.count())
//    println("用户数:"+groupUsers.count())
    println("groupUsers")
//    groupUsers.take(10).foreach(println)
//
//
////    // stage-4-2
    val ItemPair = groupUsers.map{x =>
//      val user_id = x.getAs[String]("user_id")
      val item_set = x.getAs[String]("item_set")
      val item_set_int = x.getAs[String]("item_set_int")
      val item_pair = item_set.split(",").distinct.combinations(2).toArray
      item_pair.map(y => (item_set_int,y.mkString(",")))
    }.flatMap(x => x).toDF("item_set_int","item_set")
////
//    ItemPair.take(1).foreach(println)
//    println("item_pair对个数:"+ItemPair.count())
////
////////    //  stage-5 and stage-6
    val ItemToUser = ItemPair.groupBy("item_set").
      agg(concat_ws(":",collect_set("item_set_int")).alias("item_set_int"),count("item_set_int").alias("score"))
      .filter("score>2 and score<100")
      .repartition(600)
      .drop("score")
////
    println("获取itempair的所有用户集合")
//    ItemToUser.take(1).foreach(println)

    val itemJoined = ItemToUser.map{x =>
      val item_pair = x.getAs[String]("item_set").split(",")
      var score = 0.0
      val user_set  = x.getAs[String]("item_set_int").split(":")
      for (i <- user_set.indices; j <- i + 1 until user_set.length) {
        val user_1 = user_set(i).toSet
        val user_2 = user_set(j).toSet
        score = score + 1 / (user_1.intersect(user_2).size.toDouble + 1)
      }
      (item_pair(0),item_pair(1),score)
    }.repartition(600).toDF("main_item","sim_item","score")
//    println("itemJoined")
//
//
//    println("有相似商品个数："+itemJoined.count())
//    println(itemJoined.take(10).foreach(println))
//
    val qur_main_coupon =
      s"""
         |select item_id,coupon_id,subcate_id
         |from hds.coupon_product_v2
         |where
         |status != 0
           """.stripMargin

    val subcate_coupon = spark.sql(qur_main_coupon).toDF("item","coupon","subcate_id")

    val filter_subcate = itemJoined.join(subcate_coupon,$"main_item"===$"item").
      selectExpr("cast(coupon as string) coupon","subcate_id","sim_item","score").
      toDF("main_coupon","sub1","sim_item","score").
      join(subcate_coupon,$"sim_item"===$"item")
      .select("main_coupon","coupon","sub1","subcate_id","score")
//      .filter("sub1==subcate_id")
      .repartition(600)
      .selectExpr("main_coupon","cast(coupon as string) coupon","score")
      .map(x=>(x.getAs[String]("main_coupon"),x.getAs[String]("coupon"),x.getAs[Double]("score")))
//
//    println(filter_subcate.columns.foreach(print))
//    println("过滤掉2级类目不同的商品个数："+filter_subcate.count())
//    filter_subcate.take(10).foreach(println)


    ////    //  stage-8 and stage-9
    val itemSimitem = filter_subcate.map(x => (x._2,x._1,x._3)).union(filter_subcate)
      .toDF("main_item","sim_item","score").dropDuplicates("main_item","sim_item")
      .map(x=> (x.getAs[String]("main_item"),x.getAs[String]("sim_item")+","+x.getAs[Double]("score").toString))
      .toDF("item_id","item_score")
      .groupBy("item_id")
      .agg(concat_ws(":",collect_set("item_score")).alias("item_score"))
//      .repartition(600)
//
//    println("union后商品个数："+itemSimitem.count())
//    itemSimitem.take(10).foreach(println)

    //

    val result = itemSimitem.map{x=>
      val item = x.getAs[String]("item_id")
      val item_score = x.getAs[String]("item_score").toString.split(":")
      (item,item_score.sortBy(x=>x.split(",")(1).toDouble).reverse
        .map(x=> x.split(",")(0))
        .take(200))
    }
      .filter(_._2.length > 2)
      .repartition(200)
      .map(x=>x._1+"\t"+x._2.mkString(","))

//    println("最终商品数："+result.count())
//    result.take(10).foreach(println)


    result.write.format("text").mode("overwrite").save("/user/xiaohui/swing_filter_cate")




  }

}