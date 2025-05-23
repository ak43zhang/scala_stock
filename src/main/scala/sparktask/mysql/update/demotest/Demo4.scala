package sparktask.mysql.update.demotest

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
 * 用于测试  连续5天振幅大于5个点
 */
object Demo4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.local.dir","D:\\Temp")
    val spark = SparkSession
      .builder()
      .appName("Demo4")
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val df1 = spark.read.parquet(s"file:///D:\\gsdata\\gpsj_15days\\trade_date_month=2024-0{6,7}")

    df1.createTempView("ta1")

    val df2 = spark.sql(
      """select stock_code,t0_trade_date,t3_trade_date,t8_trade_date,t8_change_pct,t9_change_pct,t10_change_pct
        |from ta1
        |where t0_sfzt=0 and t1_sfzt=0 and t2_sfzt=0 and t3_sfzt=1 and t4_sfzt=0
        |and t7_zt like '%下影线%' and least(t0_low,t1_low,t2_low,t3_low,t4_low,t5_low,t6_low,t7_low)=t7_low
        |and t0_open>=3 and t0_open <60
        |""".stripMargin)
      df2.orderBy("t0_trade_date").show(200)
    println(df2.count())

    spark.close()
  }


}
