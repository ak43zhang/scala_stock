package spark.bus.stock

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import spark.tools.MysqlProperties
import spark.bus.stock.Bus_2_SparkCollectMySql2ParquetIncrement

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
      "data_gpsj_day_20260210"
    )
    val months = "2025-12,2026-01,2026-02"
    val start_time ="2026-02-10"
    val end_time ="2026-02-10"

    Bus_2_SparkCollectMySql2ParquetIncrement.updateGpsjIncrement(Bus_2_SparkCollectMySql2ParquetIncrement.updateGpsj(spark,jyrlsSome,properties))
    Bus_3_SparkMakeWideTableIncrement.makeWide(spark,months)

    Bus_4_PressureSupportCalculator.psc(spark,properties,start_time,end_time)
    Bus_5_AdvancedDipStrategy.ad(spark,properties)

    Bus_4_PressureSupportCalculator2for40day.psc40(spark,properties,start_time,end_time)
    Bus_5_AdvancedDipStrategy2for40day.ad40(spark,properties)

    Bus_4_PressureSupportCalculator2for120day.psc120(spark,properties,start_time,end_time)
    Bus_5_AdvancedDipStrategy2for120day.ad120(spark,properties)

//    analysis_news2Parquet(spark,properties)
//    analysis_notices2Parquet(spark,properties)
//    analysis_ztb2Parquet(spark,properties)
//    analysis_area2Parquet(spark,properties)


    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }


}
