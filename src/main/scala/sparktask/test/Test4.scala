package sparktask.test

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 */
object Test4 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "10")
      .set("spark.sql.broadcastTimeout","60000")
      .set("spark.driver.memory", "6g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "10")
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

    val df = spark.read.parquet(s"file:///D:\\gsdata\\gpsj_hs_10days\\trade_date_month=2019*")
    df.createOrReplaceTempView("ta1")
    spark.sql(
      """
        |select stock_code,t0_trade_date,t0_open,t0_close,t0_high,t0_low,t0_pre_close,t1_open,t1_close,t1_high,t1_low,t1_pre_close,t1_zgzf from ta1 where t1_zgzf>11
        |""".stripMargin).show(1)

    val df2 = spark.read.parquet(s"file:///D:\\gsdata2\\gpsj_day_all_hs")
    df2.createOrReplaceTempView("ta2")
    spark.sql(
      """
        |select * from ta2 where stock_code='601225' and trade_date in('2019-07-19','2019-07-22')
        |""".stripMargin).show()



    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
  }
}
