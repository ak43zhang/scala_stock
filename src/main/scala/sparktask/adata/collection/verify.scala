package sparktask.adata.collection

import java.time.{LocalDate, LocalTime}
import java.time.DayOfWeek
import java.time.format.DateTimeFormatter
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * 验证采集数据
 */
object verify {
  def main(args: Array[String]): Unit = {
    val startm = System.currentTimeMillis()
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "20")
      .set("spark.sql.broadcastTimeout","60000")
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

    val year = "2025"
    create_table(spark,url,properties,year)
    val now_day = getFinalDate
    println(now_day)

    //验证gpsj_day_all_hs数据完整性
    println("==============验证gpsj_day_all_hs数据完整性==============")
    spark.sql(
      s"""
        |select * from data_jyrl as t1 left join gpsj_day_all_hs as t2 on t1.trade_date=t2.trade_date
        |where t2.trade_date is null and trade_status=1 and t1.trade_date like '%$year%' and t1.trade_date<='$now_day' order by t1.trade_date
        |""".stripMargin).show()

    println("==============验证gpsj_hs_10days/gpsj_hs_h10days/gpsj_hs_20days数据完整性==============")
    spark.sql(
      s"""
         |select * from data_jyrl as t1 left join gpsj_hs_10days as t2 on t1.trade_date=t2.t0_trade_date
         |where t2.t0_trade_date is null and trade_status=1 and t1.trade_date like '%$year%' and t1.trade_date<='$now_day' order by t1.trade_date
         |""".stripMargin).show()
    spark.sql(
      s"""
         |select * from data_jyrl as t1 left join gpsj_hs_h10days as t2 on t1.trade_date=t2.t0_trade_date
         |where t2.t0_trade_date is null and trade_status=1 and t1.trade_date like '%$year%' and t1.trade_date<='$now_day' order by t1.trade_date
         |""".stripMargin).show()
    spark.sql(
      s"""
         |select * from data_jyrl as t1 left join gpsj_hs_20days as t2 on t1.trade_date=t2.t0_trade_date
         |where t2.t0_trade_date is null and trade_status=1 and t1.trade_date like '%$year%' and t1.trade_date<='$now_day' order by t1.trade_date
         |""".stripMargin).show()

    //验证涨停板数据完整性
    println("==============验证涨停板数据完整性==============")
    spark.sql(
      s"""
         |select * from data_jyrl as t1 left join ztb_day as t2 on t1.trade_date=t2.trade_date
         |where t2.trade_date is null and trade_status=1 and t1.trade_date like '%$year%' and t1.trade_date<='$now_day' order by t1.trade_date
         |""".stripMargin).show()

    //验证龙虎榜每日更新数据完整性
    println("==============验证龙虎榜每日更新数据完整性==============")
    spark.sql(
      s"""
         |select * from data_jyrl as t1 left join data_lhb as t2 on t1.trade_date=t2.trade_date
         |where t2.trade_date is null and trade_status=1 and t1.trade_date like '%$year%' and t1.trade_date<='$now_day' order by t1.trade_date
         |""".stripMargin).show()

    //验证龙虎榜个股数据完整新
    println("==============验证龙虎榜个股数据完整性==============")
    spark.sql(
      s"""
         |select * from data_jyrl as t1 left join data_lhb_history2 as t2 on t1.trade_date=t2.trade_date
         |where t2.trade_date is null and trade_status=1 and t1.trade_date like '%$year%' and t1.trade_date<='$now_day' order by t1.trade_date
         |""".stripMargin).show()

    //验证股市日历每日更新数据完整性
    println("==============验证股市日历每日更新数据完整性==============")
    spark.sql(
      s"""
         |select * from data_jyrl as t1 left join data_gsdt as t2 on t1.trade_date=t2.trade_date
         |where t2.trade_date is null and trade_status=1 and t1.trade_date like '%$year%' and t1.trade_date<='$now_day' order by t1.trade_date
         |""".stripMargin).show()

    //验证压力支撑数据完整性
    println("==============验证压力支撑数据完整性==============")
    spark.sql(
      s"""
         |select * from data_jyrl as t1 left join pressure_support_calculator$year as t2 on t1.trade_date=t2.trade_date
         |where t2.trade_date is null and trade_status=1 and t1.trade_date like '%$year%' and t1.trade_date<='$now_day' order by t1.trade_date
         |""".stripMargin).show()

    //风险数据完整性验证 TODO
    println("==============风险数据完整性验证==============")
//    spark.sql("")

    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }

  def create_table(spark:SparkSession,url:String,properties: Properties,year:String): Unit ={
    val jyrldf: DataFrame = spark.read.jdbc(url, "data_jyrl", properties)
    jyrldf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    jyrldf.createOrReplaceTempView("data_jyrl")

    val ztbdf: DataFrame = spark.read.jdbc(url, s"ztb_day", properties)
        .select("trade_date").distinct()
    ztbdf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    ztbdf.createOrReplaceTempView("ztb_day")

    val gsrldf: DataFrame = spark.read.jdbc(url, s"data_gsdt", properties)
      .select("`交易日`").toDF("trade_date").distinct()
    gsrldf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    gsrldf.createOrReplaceTempView("data_gsdt")

    val lhbdf: DataFrame = spark.read.jdbc(url, s"data_lhb", properties)
      .select("trade_date").distinct()
    lhbdf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    lhbdf.createOrReplaceTempView("data_lhb")

    val lhb2df: DataFrame = spark.read.jdbc(url, s"data_lhb_history2", properties)
      .select("trade_date").distinct()
    lhb2df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    lhb2df.createOrReplaceTempView("data_lhb_history2")

    val news_financial_df: DataFrame = spark.read.jdbc(url, "news_financial", properties)
    news_financial_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    news_financial_df.createOrReplaceTempView("news_financial")

    val news_cls_df: DataFrame = spark.read.jdbc(url, "news_cls", properties)
    news_cls_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    news_cls_df.createOrReplaceTempView("news_cls")

    val news_combine_df: DataFrame = spark.read.jdbc(url, "news_combine", properties)
    news_combine_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    news_combine_df.createOrReplaceTempView("news_combine")

    spark.read.parquet(s"file:///D:\\gsdata\\gpsj_day_all_hs\\trade_date_month=202*")
    .select("trade_date").distinct()
    .createOrReplaceTempView("gpsj_day_all_hs")

    spark.read.parquet(s"file:///D:\\gsdata\\gpsj_hs_10days\\trade_date_month=202*")
      .select("t0_trade_date").distinct()
      .createOrReplaceTempView("gpsj_hs_10days")

    spark.read.parquet(s"file:///D:\\gsdata\\gpsj_hs_h10days\\trade_date_month=202*")
      .select("t0_trade_date").distinct()
      .createOrReplaceTempView("gpsj_hs_h10days")

    spark.read.parquet(s"file:///D:\\gsdata\\gpsj_hs_20days\\trade_date_month=202*")
      .select("t0_trade_date").distinct()
      .createOrReplaceTempView("gpsj_hs_20days")

    val stockDF  = spark.read.jdbc(url, s"pressure_support_calculator$year", properties)
    stockDF.createOrReplaceTempView(s"pressure_support_calculator$year")
  }


  // 判断当前时间是否在下午3点之后
  def isAfter3PM(): Boolean = {
    val currentTime = LocalTime.now()
    val cutoffTime = LocalTime.of(15, 0) // 下午3点
    currentTime.isAfter(cutoffTime)
  }

  // 获取上一个交易日（假设交易日为周一到周五）
  def getPreviousTradingDay(date: LocalDate): LocalDate = {
    var previousDay = date.minusDays(1)
    while (previousDay.getDayOfWeek == DayOfWeek.SATURDAY || previousDay.getDayOfWeek == DayOfWeek.SUNDAY) {
      previousDay = previousDay.minusDays(1)
    }
    previousDay
  }

  // 获取最终日期
  def getFinalDate(): String = {
    val currentDate = LocalDate.now()
    val finalDate = if (isAfter3PM()) {
      currentDate
    } else {
      getPreviousTradingDay(currentDate)
    }
    finalDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
  }
}
