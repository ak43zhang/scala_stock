package sparktask.test

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import sparktask.tools.MysqlTools
import scala.collection.mutable.ArrayBuffer

object ReadTest1_v4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "200")
      .set("spark.driver.memory", "8g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "10")
      .set("spark.sql.parquet.enableVectorizedReader","false")
      .set("spark.local.dir", "D:\\SparkTemp")

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val url = "jdbc:mysql://localhost:3306/gs"
    val driver = "com.mysql.cj.jdbc.Driver"
    val user = "root"
    val pwd = "123456"

    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", pwd)
    properties.setProperty("url", url)
    properties.setProperty("driver", driver)

    val startm = System.currentTimeMillis()
//    val setdate = "2025-04-15"
//    val setdate_ = setdate.replaceAll("-","")
//    val year = setdate.substring(0,4)

    val start_time ="2025-01-01"
    val end_time ="2025-04-16"

    val data20_df = spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_10days\\trade_date_month=20[25]*")//19,20,21,22,23,24,
        .select("t2_trade_date","stock_code","t2_kpzf","t2_zgzf","t2_zdzf","t2_spzf")
        .where(s"t2_trade_date between '$start_time' and '$end_time'")
    data20_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    data20_df.createOrReplaceTempView("d1")

    val result_expect_risk1_df: DataFrame = spark.read.jdbc(url, s"result_expect_risk1", properties)
    result_expect_risk1_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    result_expect_risk1_df.createOrReplaceTempView("r1")

    val result_expect_risk2_df: DataFrame = spark.read.jdbc(url, s"result_expect_risk2", properties)
    result_expect_risk2_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    result_expect_risk2_df.createOrReplaceTempView("r2")

    val result_expect_normal_df: DataFrame = spark.read.jdbc(url, s"result_expect_normal", properties)
    result_expect_normal_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    result_expect_normal_df.createOrReplaceTempView("n1")

    val data_agdm_df: DataFrame = spark.read.jdbc(url, s"data_agdm", properties)
    data_agdm_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    data_agdm_df.createOrReplaceTempView("data_agdm")

    val dsz_df: DataFrame = spark.read.jdbc(url, s"dashizhi_20250224", properties)
    dsz_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    dsz_df.createOrReplaceTempView("dashizhi_20250224")



    //指数均值
//    val agdm_df = spark.sql(
//      """
//        |select stock_code from data_agdm where (stock_code like '00%' or stock_code like '60%' ) and short_name not like '%退%' and short_name not like '%ST%' and list_date is not null and stock_code not in (select `代码` from dashizhi_20250224 )
//        |""".stripMargin).createOrReplaceTempView("agdm")
//
//    val zsjz_df = spark.sql(
//      """
//        |select sum(t2_kpzf)/count(1) as z1k,sum(t2_spzf)/count(1) as z1s,t2_trade_date from agdm left join d1 on agdm.stock_code = d1.stock_code group by t2_trade_date order by t2_trade_date desc
//        |""".stripMargin)
//
//    zsjz_df.show()


//    val r1_df = spark.sql(
//      """
//        |select sum(t2_kpzf)/count(1) as r1k,sum(t2_spzf)/count(1) as r1s,d1.t2_trade_date as td from d1 left join r1 on d1.t2_trade_date=r1.trade_date and d1.stock_code=r1.dm
//        |where dm is not null
//        |group by d1.t2_trade_date
//        |order by d1.t2_trade_date
//        |""".stripMargin)
//    r1_df.show()
//    r1_df.createOrReplaceTempView("r1m")
//
//    val r2_df = spark.sql(
//      """
//        |select sum(t2_kpzf)/count(1) as r2k,sum(t2_spzf)/count(1) as r2s,d1.t2_trade_date as td from d1 left join r2 on d1.t2_trade_date=r2.t2_trade_date and d1.stock_code=r2.dm
//        |where dm is not null
//        |group by d1.t2_trade_date
//        |order by d1.t2_trade_date
//        |""".stripMargin)
//    r2_df.show()
//    r2_df.createOrReplaceTempView("r2m")
//
//    val n1_df = spark.sql(
//      """
//        |select sum(t2_kpzf)/count(1) as n1k,sum(t2_spzf)/count(1) as n1s,d1.t2_trade_date as td from d1 left join n1 on d1.t2_trade_date=n1.t2_trade_date and d1.stock_code=n1.dm
//        |where dm is not null
//        |group by d1.t2_trade_date
//        |order by d1.t2_trade_date
//        |""".stripMargin)
//    n1_df.show()
//    n1_df.createOrReplaceTempView("n1m")
//
//    val mid_df = spark.sql(
//      """
//        |select r1k,r2k,(r1k+r2k)/2 as r3k,n1k,r1s,r2s,(r1s+r2s)/2 as r3s,n1s,r1m.td from r1m left join r2m on r1m.td=r2m.td left join n1m on r1m.td=n1m.td
//        |order by n1m.td desc
//        |""".stripMargin)
//    mid_df.show(300)
//    mid_df.createOrReplaceTempView("mid")
//
//    val columnsList1 = ArrayBuffer[String]("stock_code", "t0_trade_date",  "t0_kpzf", "t0_spzf")
//    val d2 = spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_10days\\trade_date_month=202[4,5]*") //
//      .select(columnsList1.map(col): _*)
//      .where(s"t0_trade_date between '$start_time' and '$end_time'")
//    d2.createOrReplaceTempView("d2")
//
//    spark.sql(
//      s"""
//         |select sum(t0_kpzf)/count(1) as z1k,sum(t0_spzf)/count(1) as z1s,
//         |sum(if(t0_kpzf>=0,1,0)) as d1k,sum(if(t0_kpzf<0,1,0)) as x1k,
//         |sum(if(t0_spzf>=0,1,0)) as d1s,sum(if(t0_spzf<0,1,0)) as x1s,
//         |count(1) as z,t0_trade_date as td2 from d2
//         |group by t0_trade_date
//         |order by t0_trade_date
//         |""".stripMargin).createOrReplaceTempView("mid2")
//
//    spark.sql(
//      """
//        |select * from mid left join mid2 on mid.td=mid2.td2
//        |""".stripMargin).drop("td2").show(200)
//
//
//    val mid3_df = spark.sql(
//      """
//        |select if(n1k>r3k,'n1k','r3k') as k,if(n1s>r3s,'n1s','r3s') as s from mid
//        |""".stripMargin)
//
//    mid3_df.show()
//    mid3_df.createOrReplaceTempView("mid3")
//
//    spark.sql(
//      """
//        |select k,count(1) as c from mid3 group by k
//        |order by k
//        |""".stripMargin).show()
//
//    spark.sql(
//      """
//        |select s,count(1) as c from mid3 group by s
//        |order by s
//        |""".stripMargin).show()


    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }
}
