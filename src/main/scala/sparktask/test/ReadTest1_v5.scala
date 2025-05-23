package sparktask.test

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import sparktask.tools.MysqlTools

import scala.collection.mutable.ArrayBuffer

object ReadTest1_v5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "200")
      .set("spark.driver.memory", "16g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "10")
      .set("spark.sql.parquet.enableVectorizedReader", "false")
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

    //    val setdate = "2025-04-15"
    //    val setdate_ = setdate.replaceAll("-","")
    //    val year = setdate.substring(0,4)
    val startm = System.currentTimeMillis()

    val start_time = "2025-01-01"
    val end_time = "2025-04-16"

    val columnsList = ArrayBuffer[String]("stock_code", "t0_trade_date",  "t0_kpzf", "t0_spzf")
    val data20_df = spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_10days\\trade_date_month=202[4,5]*") //
      .select(columnsList.map(col): _*)
        .where(s"t0_trade_date between '$start_time' and '$end_time'")
    data20_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    data20_df.createOrReplaceTempView("d")

    val d2 = spark.sql(
      s"""
         |select sum(t0_kpzf)/count(1) as z1k,sum(t0_spzf)/count(1) as z1s,count(1) as z,t0_trade_date from d
         |group by t0_trade_date
         |""".stripMargin)
      d2.show(200)
    d2.createOrReplaceTempView("d2")

    val d3 = spark.sql(
      """
        |select if(z1k>z1s,'z1k','z1s') as c from d2
        |""".stripMargin)
      d3.show()
    d3.createOrReplaceTempView("d3")

    spark.sql("select c,count(1) from d3 group by c").show()

    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")

    spark.close()
  }
}
