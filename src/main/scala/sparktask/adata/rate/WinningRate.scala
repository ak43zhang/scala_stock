package sparktask.adata.rate

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * 胜率统计
 */
object WinningRate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions","10")
      .set("spark.driver.memory","8g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "10")
      .set("spark.local.dir","D:\\SparkTemp")

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val startm = System.currentTimeMillis()

    val df2 = spark.read.parquet("file:///D:\\gsdata\\gpsj_day_all_hs\\trade_date_month=2024*")
    df2.cache()
    df2.createTempView("ta1")

    val df3 = spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_20days\\trade_date_month=2024*")
    df3.cache()
    df3.createTempView("ta2")

    threeDay2Eight(spark)

    val endm = System.currentTimeMillis()
    println("共耗时："+(endm-startm)/1000+"秒")
    spark.close()
  }

  /**
   * 3天博两版，8个点买入胜率
   *
   * 前10个交易日最低值和涨停板的区间涨跌幅
   *
   * @param spark
   */
  def threeDay2Eight(spark:SparkSession): Unit ={
    val df = spark.sql(
      """
        |select *,round((t10_high-m10)/m10*100,2) as r,(t12_spzf-8+t13_kpzf) as jjp from
        |(select stock_code,
        |least(t0_low,t1_low,t2_low,t3_low,t4_low,t5_low,t6_low,t7_low,t8_low,t9_low) as m10,t10_high,
        |t10_trade_date,t12_kpzf,t12_spzf,t12_zgzf,t12_zdzf ,t13_kpzf,t13_spzf,t13_zgzf,t13_zdzf,t13_sfzt
        |from ta2
        |where t8_sfzt=0 and t9_sfzt=0 and t10_sfzt=1 and t11_sfzt=0 and t12_zgzf>t11_zgzf  and t12_zgzf>9)
        | order by t10_trade_date
        |""".stripMargin)
      df.show(2000)
//    .orderBy(col("r").desc)
//    println(df.filter("t13_zgzf>1").count())
//    println(df.filter("t13_zgzf<0").count())
//    println(df.filter("t13_sfzt=1").count())

    println(df.filter("t13_kpzf>0 and jjp>0").count())
    println(df.filter("t13_kpzf>0 and jjp<0").count())
    println(df.filter("t13_kpzf<0 and jjp>0").count())
    println(df.filter("t13_kpzf<0 and jjp<0").count())

  }


}
