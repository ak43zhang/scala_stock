package sparktask.adata.statistics

import java.time.format.DateTimeFormatter
import java.time.{DayOfWeek, LocalDate, LocalTime}
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import sparktask.tools.MysqlTools

import scala.collection.mutable.ArrayBuffer

/**
 * 查询基础数据 日的数据存入年
 */
object BaseDay2Year {
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

//    val year = "2023"

//    val now_day = getFinalDate
//    println(now_day)

    val jyrls = spark.read.jdbc(url, "data_jyrl", properties).where(s"trade_status=1 and trade_date between '2017-01-01' and '2024-01-01'")
      .select("trade_date").collect().map(f => f.getAs[String]("trade_date")).toList

    for (jyrl <- jyrls) {
      val day = jyrl.replaceAll("-","")
      val year = jyrl.substring(0,4)
      val input_table = s"wencaiquery_basequery_$day"
      val output_table = s"wencaiquery_basequery_$year"

      println(s"$day===$year===$input_table===$output_table")

      val df = spark.read.jdbc(url, input_table, properties)
      df.createOrReplaceTempView("basequery")
      val df2 = spark.sql(s"select *,'$jyrl' as trade_date from basequery")

      //删除日期内的条件数据并重写
      try {
        // 通过 Spark 执行 SQL 删除语句
        val deleteQuery = s"DELETE FROM ${output_table} WHERE trade_date='$jyrl'"
        MysqlTools.mysqlEx(output_table, deleteQuery)
        //        println("数据删除成功！")
      } catch {
        case e: Exception => println(s"数据删除失败: ${e.getMessage}")
      }

      df2.write.mode("append").jdbc(url, output_table, properties)
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
