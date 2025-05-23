package sparktask.adata.indicators

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._


/**
 * 历史板块指标分析
 */
object SectorAnalysis4History {
  def main(args: Array[String]): Unit = {
    val startm = System.currentTimeMillis()
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "20")
      .set("spark.sql.broadcastTimeout", "60000")
      .set("spark.driver.memory", "8g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "20")
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


    createTable(spark, url, properties)

    val listsetDate = spark.sql("select trade_date from data_jyrl where  trade_date between '2025-03-28' and '2025-03-28' and trade_status='1' order by trade_date desc")
      .collect()
      .map(f => f.getAs[String]("trade_date"))

    for (setday <- listsetDate) {
      val setdate = setday.replaceAll("-", "")
      val formattedDate = setday

      val cycledfArray = spark.sql(s"select trade_date from data_jyrl where trade_status=1 and trade_date<'$formattedDate' group by trade_date order by trade_date desc limit 200")
        .collect().map(f => f.getAs[String]("trade_date"))
      val beforedate = cycledfArray(0)
      val before2date = cycledfArray(2)
      val before15date = cycledfArray(15)
      val before40date = cycledfArray(40)
      val before120date = cycledfArray(119)
      println("当前日期:" + setdate + "\n上一个交易日：" + beforedate + "\n涨停筛选区间：" + before15date + "到" + before2date)
      //数据统一生成表

      ztb_sector(spark,beforedate)
    }

    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }

  /**
   * 涨停股所在板块分布
   */
  def ztb_sector(spark: SparkSession, beforedate:String): Unit = {
    spark.sql(
      s"""
        |select * from ztb_day as t1 left join data_dzgpssgn_baidu as t2 on t1.`股票代码`=t2.stock_code where trade_date='${beforedate}' and name is not null
        |""".stripMargin).createOrReplaceTempView("midtable_baidu")

    spark.sql(
      s"""
         |select * from ztb_day as t1 left join data_dzgpssgn_ths as t2 on t1.`股票代码`=t2.stock_code where trade_date='${beforedate}' and name is not null
         |""".stripMargin).createOrReplaceTempView("midtable_ths")

    spark.sql(
      s"""
         |select * from ztb_day as t1 left join data_dzgpssgn_east as t2 on t1.`股票代码`=t2.stock_code where trade_date='${beforedate}' and plate_name is not null
         |""".stripMargin).createOrReplaceTempView("midtable_east")


    println("==============================板块涨停个股昨日主热点===============================")
    val bk1 = spark.sql(
      """
        |select name,count(1) as count from midtable_baidu
        |group by name order by count(1) desc
        |""".stripMargin)

    val bk2 = spark.sql(
      """
        |select name,count(1) as count from midtable_ths
        |group by name order by count(1) desc
        |""".stripMargin)

    val bk3 = spark.sql(
      """
        |select plate_name as name,count(1) as count from midtable_east
        |group by plate_name order by count(1) desc
        |""".stripMargin)

    bk1.union(bk2).union(bk3).groupBy("name").agg(sum("count").alias("total_count")).orderBy(desc("total_count")).show()

    println("==============================板块连板个股昨日主热点===============================")
    //TODO



  }

  /**
   * 数据统一生成表
   *
   * @param spark
   * @param url
   * @param properties
   */
  def createTable(spark: SparkSession, url: String, properties: Properties): Unit = {
    val jyrldf: DataFrame = spark.read.jdbc(url, "data_jyrl", properties)
    jyrldf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    jyrldf.createOrReplaceTempView("data_jyrl")

    val ztbdf: DataFrame = spark.read.jdbc(url, "ztb_day", properties)
    ztbdf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    ztbdf.createOrReplaceTempView("ztb_day")

    val lhbdf: DataFrame = spark.read.jdbc(url, "data_lhb_history2", properties)
    lhbdf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    lhbdf.createOrReplaceTempView("data_lhb_history2")

    //百度
    val bk_baidu_df = spark.read.jdbc(url, "data_dzgpssgn_baidu", properties)
      .where("name not in ('昨日涨停','转融券标的','业绩反转','国企改革','昨日连板','微盘股概念','沪股通','ST概念','昨日触板','融资融券','深股通','创投')")
    bk_baidu_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    bk_baidu_df.createOrReplaceTempView("data_dzgpssgn_baidu")

    //同花顺
    val bk_ths_df = spark.read.jdbc(url, "data_dzgpssgn_ths", properties)
      .where("name not in ('昨日涨停','转融券标的','业绩反转','国企改革','昨日连板','微盘股概念','沪股通','ST板块','昨日触板','融资融券','深股通','创投')")
    bk_ths_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    bk_ths_df.createOrReplaceTempView("data_dzgpssgn_ths")

    //东方财富
    val bk_east_df = spark.read.jdbc(url, "data_dzgpssgn_east", properties)
          .where("plate_type='概念' and plate_name not in ('昨日涨停_含一字','昨日涨停','预亏预减','昨日连板_含一字','融资融券','机构重仓','预盈预增','昨日连板','ST股','沪股通','央国企改革','QFII重仓','深股通','创投')")
    bk_east_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    bk_east_df.createOrReplaceTempView("data_dzgpssgn_east")
  }

  /**
   *
   */

}
