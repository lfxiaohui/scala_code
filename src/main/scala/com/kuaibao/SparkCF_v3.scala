package com.kuaibao

import java.io.{File, FileInputStream}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.json4s._
import org.json4s.jackson.Serialization._
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.beans.BeanProperty
import scala.util.control.Breaks._

class CFconfig {
    @BeanProperty var appName = "Uni_CF"
    @BeanProperty var simFilePath = ""
    @BeanProperty var simFileType = ""
    @BeanProperty var days = -1
    @BeanProperty var joinKey = ""
    @BeanProperty var userAct_sql = ""
    @BeanProperty var validCoupon_sql = ""
    @BeanProperty var midPath = ""
    @BeanProperty var resultPath = ""
    @BeanProperty var toHDFS = false
    @BeanProperty var saveCount = 300
    @BeanProperty var owner = ""
    @BeanProperty var minLimit = ""
    @BeanProperty var maxLimit = ""

    override def toString: String = s"appName: $appName, simFilePath: $simFilePath, owner: $owner"
}

object SparkCF_v3 {

    Logger.getLogger("org").setLevel(Level.WARN)

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf()
        sparkConf.set("spark.dynamicAllocation.maxExecutors", "16")

        val spark = SparkSession.builder().appName("PersonalRec").enableHiveSupport().config(sparkConf).getOrCreate()
        val filename = args(0)
        val input = new FileInputStream(new File(filename))
        //    val input = Source.fromFile(filename).getLines().next()

        val yaml = new Yaml(new Constructor(classOf[CFconfig]))
        val e = yaml.load(input).asInstanceOf[CFconfig]

        //sql替换
        val formatter = new SimpleDateFormat("yyyyMMdd")
        val c1: Calendar = Calendar.getInstance()
        val c2: Calendar = Calendar.getInstance()
        c1.add(Calendar.DATE, -1)
        c2.add(Calendar.DATE, e.days)
        val t1 = formatter.format(c1.getTime)
        val t2 = formatter.format(c2.getTime)
        val sql = e.userAct_sql.replaceAll("\\{STARTDATE\\}", t2).replaceAll("\\{ENDDATE\\}", t1)
        println(e)
        e.userAct_sql = sql
        couponBasedCF(spark, e)

        spark.stop()
    }

    def couponBasedCF(spark: SparkSession, cfconfig: CFconfig): Unit = {
        import spark.implicits._

        val saveCount = cfconfig.saveCount
        val sql = cfconfig.userAct_sql
        //用户点击序列
        val clickDF = spark.sqlContext.sql(sql)
        val joinKey = cfconfig.joinKey // 用那个key关联行为和相似商品，比如itemid/couponid/cateid

        //过滤有效商品
        val sql2 = cfconfig.validCoupon_sql
        val rdd_valid = spark.sqlContext.sql(sql2).map(x => (x.getAs[String]("itemId"), "1")).rdd.map(x => (String.valueOf(x._1), x._2)).cache()

        // 读相似度文件
        val couponSimFile = cfconfig.simFilePath
        val SimFileType = cfconfig.simFileType
        val ds = spark.read.textFile(couponSimFile)
        var rdd_sim = ds.map(_.split("\t")).filter(_.length == 3).rdd.map(x => (x(1), (x(0), x(2))))


        // join过滤有效商品
        val rdd_join = rdd_sim.join(rdd_valid).map(x => (x._2._1._1, (x._1, x._2._1._2))).groupByKey.map(x => (x._1, x._2.toList.sortBy(_._2).reverse.take(400)))
        val ItemSimDF = rdd_join.map(x => (x._1, x._2.map(x => x._1 + ":" + x._2).mkString(","))).toDF(joinKey, "simList")


        //join click and coupon sim
        val df0 = ItemSimDF.join(clickDF, joinKey).select($"deviceId", ItemSimDF(joinKey), $"clickCount", $"simList", $"time", $"trigger_type")

        val dfWithCount = df0.groupBy("deviceId").count() //每个人点过几个商品

        val df1 = df0.join(dfWithCount, "deviceId").select($"deviceId", df0(joinKey), $"count".as("couponCount"), $"clickCount", $"simList", $"time", $"trigger_type")
        val toHdfs = cfconfig.toHDFS

        val midPath = cfconfig.midPath
        //		if(toHdfs){
        //			df1.write.format("csv")
        //				.option("delimiter", "\t").mode("overwrite")
        //				.save(midPath)
        //		}
        val days = cfconfig.days
        val c2: Calendar = Calendar.getInstance()
        c2.add(Calendar.DATE, days)

        val decay = 0.92
        val minLimit = cfconfig.minLimit.toInt
        val maxLimit = cfconfig.maxLimit.toInt
        val appName = cfconfig.appName
        //每一个有行为商品推荐出的list（一个用户可能有若干个）
        val proScore = udf { (couponCount: Int, couponId: Long, clickCount: Int, simList: String, time: Int, trigger_type: String) => {
            val fields = simList.split(",") //couponId有哪些相似商品
            var res_length = 0
            val clickCountSqrt = math.sqrt(clickCount)

            var timeGap = (time - c2.getTimeInMillis / 1000) / 86400
            var weight: Double = timeGap

            if (time == 0) {
                weight = 0.0f
                timeGap = 0
            }

            var limit = 1 + saveCount / couponCount
            if (appName == "i2vCF") {
                var limit = 1 + saveCount / couponCount * clickCountSqrt
                limit = (math.pow(decay, timeGap) * 1).toInt
                if (limit < 7 && timeGap < 15) {
                    limit = 7
                }
            }
            //
            //      if(limit < 10 && timeGap < 15){
            //        limit = 10
            //      }else
            if (limit < minLimit) {
                limit = minLimit
            }


            var rec_list: Array[String] = Array()
            breakable {
                for (f <- fields) {
                    res_length += 1
                    if (res_length >= limit) {
                        break
                    }
                    val kv = f.split(":") // k,v:couponId,score
                    if (kv.size == 2) {
                        var initScore = kv(1).toDouble
                        if (!initScore.isNaN) {
                            //tc:trigger_coupon,rc:recommend_coupon,tt:trigger_type,s:score
                            val ele = kv(0) + ":" + (initScore + weight).toString
                            rec_list = rec_list :+ ele
                        }
                    }
                }
            }
            val res = Map("tr" -> couponId.toString, "ac" -> trigger_type, "ts" -> time.toString, "it" -> rec_list.mkString(","))
            res
        }
        }

        val df2 = df1.withColumn("simList2", proScore($"couponCount", $"$joinKey", $"clickCount", $"simList", $"time", $"trigger_type"))

        //group sim list by deviceId
        val df3 = df2.groupBy("deviceId").agg(collect_list($"simList2").as("simList2"))

        // simList 每一个元素("tc"->couponId.toString,"tt"->trigger_type,"t"->time.toString,"rl"->rec_list.mkString(","))
        val sortScore2 = udf { (simList: Seq[Map[String, String]]) => {
            var ret: List[Map[String, Any]] = List()
            val sortSimList = simList.sortBy(x => (x.getOrElse("ac", "0"), x.getOrElse("ts", "0")))(Ordering.Tuple2(Ordering.String.reverse, Ordering.String.reverse))
            // 对每一个rl内排序

            var fieldList: List[String] = List() // 每个人一个商品set，去重

            for (ele <- sortSimList) {
                //				var sortedList: List[Map[String, String]] = List()
                var sortedList: List[String] = List()

                val split1 = ele.get("it").get.split(",")
                val split2 = split1.map(x => x.split(":")).filter(x => x.length == 2)
                val split3 = split2.map { case Array(f1, f2) => (f1, f2) }
                val sortedFields = split3.toList.sortWith(_._2.toDouble > _._2.toDouble)

                for (i <- sortedFields.indices) {
                    val couponId = sortedFields(i)._1
                    if (fieldList.lengthCompare(saveCount) < 0 && !fieldList.contains(couponId)) {
                        fieldList = fieldList :+ couponId
                        //						sortedList = sortedList :+ Map("rc" -> sortedFields(i)._1, "s" -> sortedFields(i)._2)
                        sortedList = sortedList :+ sortedFields(i)._1

                    }
                }
                val _ele = ele ++ Map("it" -> sortedList)
                ret = ret :+ _ele
            }

            implicit val formats = DefaultFormats
            val jsonString = write(ret)
            jsonString
        }
        }
        val df4 = df3.withColumn("simList3", sortScore2($"simList2"))

        val resultPath = cfconfig.resultPath
        if (toHdfs) {
            df4.select("deviceId", "simList3").write.format("csv")
                    .option("delimiter", "\t").mode("overwrite").save(resultPath)
        }
    }
}
