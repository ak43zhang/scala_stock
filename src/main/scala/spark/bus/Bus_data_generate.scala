package spark.bus

import java.time.format.DateTimeFormatter
import java.time.{DayOfWeek, LocalDate, LocalTime}
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import spark.ParameterSet
import spark.bus.Bus_2_SparkCollectMySql2ParquetIncrement.{updateGpsj, updateGpsjIncrement}
import spark.bus.Bus_3_SparkMakeWideTableIncrement.makeWide
import spark.bus.Bus_4_PressureSupportCalculator.psc
import spark.bus.Bus_4_PressureSupportCalculator2for40day.psc40
import spark.bus.Bus_4_PressureSupportCalculator2for120day.psc120
import spark.bus.Bus_5_AdvancedDipStrategy.ad
import spark.bus.Bus_5_AdvancedDipStrategy2for40day.ad40
import spark.bus.Bus_5_AdvancedDipStrategy2for120day.ad120
import spark.bus.Bus_6_AiAnalysis2WideTable.{analysis_area2Parquet, analysis_news2Parquet, analysis_notices2Parquet, analysis_ztb2Parquet}
import spark.tools.MysqlProperties

import scala.collection.mutable.ArrayBuffer


/**
 * 数据生成
 */
object Bus_data_generate {
  def main(args: Array[String]): Unit = {
    val startm = System.currentTimeMillis()
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "10")
      .set("spark.sql.broadcastTimeout","60000")
      .set("spark.driver.memory", "4g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "10")
      .set("spark.local.dir", "D:\\SparkTemp")

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val properties = MysqlProperties.getMysqlProperties()

    //参数设置
    val jyrlsSome = ArrayBuffer(
      "data_gpsj_day_20260206"
    )
    val months = "2025-12,2026-01,2026-02"
    val start_time ="2026-02-06"
    val end_time ="2026-02-06"

    updateGpsjIncrement(updateGpsj(spark,jyrlsSome,properties))
    makeWide(spark,months)

    psc(spark,properties,start_time,end_time)
    ad(spark,properties)

    psc40(spark,properties,start_time,end_time)
    ad40(spark,properties)

    psc120(spark,properties,start_time,end_time)
    ad120(spark,properties)

//    analysis_news2Parquet(spark,properties)
//    analysis_notices2Parquet(spark,properties)
//    analysis_ztb2Parquet(spark,properties)
//    analysis_area2Parquet(spark,properties)


    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }


}
