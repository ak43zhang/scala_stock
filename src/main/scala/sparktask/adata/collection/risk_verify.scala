package sparktask.adata.collection

import java.time.format.DateTimeFormatter
import java.time.{DayOfWeek, LocalDate, LocalTime}
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
 * 风险验证
 */
object risk_verify {
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


    //    jyrls.foreach(println)
            val years = ArrayBuffer("2015","2016","2017","2018","2019","2020","2021","2022","2023","2024","2025")
//    val years = ArrayBuffer("2025")

    for (year <- years) {
      val df = spark.read.jdbc(url, s"wencaiquery_venture_$year", properties)
      df.createOrReplaceTempView("venture")
      val jyrls = spark.read.jdbc(url, "data_jyrl", properties).where(s"trade_status=1 and trade_date between '2025-04-18' and '2025-04-21' and trade_date like '$year%'")
        .select("trade_date").collect().map(f => f.getAs[String]("trade_date")).toList
      for (jyrl <- jyrls) {
        val risks = ArrayBuffer("公告风险", "股份质押", "股东减持", "股东大会", "商誉占比", "预约披露时间_东方财富", "预约披露时间_巨潮资讯", "限售股解禁", "大宗交易", "高管减持", "业绩预告", "流动性风险_换手", "流动性风险_换手2", "立案调查", "分红派息", "监管日期", "融资余额风险")
        for (risk <- risks) {
          val count = spark.sql(s"select count(1) as c from venture where trade_date='$jyrl' and `风险类型`='$risk'").collect().map(f => f.getAs[Long]("c")).head
          //            println(count)
          if (count == 0) {
            println(s"==========$jyrl=============$risk=============")
//            spark.sql(s"select '$jyrl' as jyrl,'$risk' as risk").write.mode("append").jdbc(url,"lost_risk",properties)
          }
        }
      }
    }


    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
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
