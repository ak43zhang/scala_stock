package sparktask.test

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

object ReadParquet {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.scheduler.listenerbus.eventqueue.capacity", "100000")
      .set("spark.scheduler.listenerBus.eventQueue.size", "100000")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.driver.memory", "8g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "1")
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

    import spark.implicits._
    //    spark.read.parquet("file:///D:\\gsdata\\gpsj_day_all_hs\\trade_date_month=2025-02")
    //      .where("stock_code='002036'")
    //      .orderBy(col("trade_time").desc)
    //        .show()


    //    spark.read.parquet("file:///D:\\gsdata\\pressure_support_calculator\\valid_results_pressure_advanced_dip_strategy")
    //      .where("stock_code in ('000679') and trade_time like '2024-07%'")
    ////      .select("windowSize").distinct()
    //      .orderBy(col("trade_time").desc)
    //      .show(600)

    //    spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_10days\\trade_date_month=2024-07").where("stock_code='000679'").show(40)




      //for(year<-2023 to 2024) {
      val df = spark.read.jdbc(url, "fx002_v5", properties)
        .where(s"t1_trade_date between '2023-01-01' and '2024-09-15'")
        //      .where(
        //        """t1_trade_date like '%2024-01%' or
        //          |t1_trade_date like '%2024-02%' or
        //          |t1_trade_date like '%2024-03%' or
        //          |t1_trade_date like '%2024-04%' or
        //          |t1_trade_date like '%2024-05%' or
        //          |t1_trade_date like '%2024-06%' or""".stripMargin)
        //      .where(
        //        """t1_trade_date like '%2024-07%' or
        //          |t1_trade_date like '%2024-08%' or
        //          |t1_trade_date like '%2024-09%' or
        //          |t1_trade_date like '%2024-10%' or
        //          |t1_trade_date like '%2024-11%' or
        //          |t1_trade_date like '%2024-12%' or""".stripMargin)
        .distinct()
      df.cache()
      df.createOrReplaceTempView("ta1")

      for (t1_kpzf <- 0 to 5) {
        var df11: DataFrame = null

        /**
         * bugzdf 购买涨跌幅
         * channel_position 通道位置 越低胜率越高
         * t1_kpzf desc   有限购买开盘涨幅大的
         *
         */
        //    val channel_position = 40
        //    val bugzdf = 5
            val row = 1
        //    val t1_kpzf = 5

        //    for (t1_kpzf <- 4 to 4) {
        //      for (row <- 1 to 1) {
        //        for (channel_position <- 40 to 40 by 10) {
        //          for (bugzdf <- 4 to 4) {


//        for (row <- 1.0 to 8.0 by 1.0) {
          for (channel_position <- 10 to 90 by 10) {
            for (bugzdf <- 5 to 10) {

              //            println("t1_kpzf>>"+t1_kpzf)
              //            println("row>>"+row)
              //            println("channel_position>>"+channel_position)
              //            println("bugzdf>>" + bugzdf)

              val df2 = spark.sql(
                s"""
                   |select * from
                   |(select stock_code,trade_time,windowSize,close,composite_support,composite_pressure,support_ratio,pressure_ratio,channel_position,t1_trade_date,t1_sfzt,t1_cjzt,
                   |t1_kpzf,t1_zgzf,t1_spzf,t2_kpzf,t2_zgzf,t2_zdzf,t2_spzf,
                   |cast(t1_spzf-$bugzdf+t2_kpzf as double) as jjyk,
                   |cast(t1_spzf-$bugzdf+t2_zgzf as double) as zgyk,
                   |cast(t1_spzf-$bugzdf+(t2_kpzf+t2_zgzf+t2_spzf)/3 as double) as pjyk,
                   |cast(t1_spzf-$bugzdf+t2_spzf as double) as spyk,
                   |cast(case when t2_kpzf<-2 then t1_spzf-$bugzdf+t2_kpzf
                   |  when t2_kpzf>-2 then (t2_kpzf+t2_zgzf)/2
                   |  else t1_spzf-$bugzdf+t2_kpzf
                   |  end as double) as yk
                   |,row_number() over(partition by t1_trade_date order by t1_trade_date asc,t1_kpzf ) as row from ta1
                   |where t1_kpzf>=$t1_kpzf-1 and t1_kpzf<$t1_kpzf and t1_zgzf>=$bugzdf and $bugzdf>=t1_kpzf  and channel_position>=$channel_position-10 and channel_position <$channel_position)
                   | where row<=$row
                   |order by trade_time
                   |""".stripMargin)
              //            df2.show(4000)
              df2.createOrReplaceTempView("ta2")


              val df3 = spark.sql("select t1_trade_date as date,collect_list(jjyk) as jjyks from ta2 group by t1_trade_date order by t1_trade_date")
              df3.createOrReplaceTempView("ta3")
              //      df3.show(false)

              // 初始本金
              var initialCapital = 10000.0

              // 按日期排序并收集到本地（假设数据量不大）
              val sortedDates = df3.orderBy("date")
                .select("date", "jjyks")
                .as[(String, Array[Double])]
                .collect()

              // 计算每天的累计总金额
              val results = sortedDates.map { case (date, changes) =>
                // 计算当天的交易影响总和
                val dailyChange = changes.map { change =>
                  (initialCapital / row) * (change / 100.0) // 每笔交易的影响
                }.sum
                // 更新总金额
                initialCapital += dailyChange
                (date, dailyChange, initialCapital)
              }

              // 转换为 DataFrame 输出
//              val resultDF = spark.createDataFrame(results).toDF("date", "daily_change", "total_capital")
              //            resultDF.orderBy("date").show(1000, false)

              val middf = spark.sql(
                s"""select
                   |$row as row,
                   |$t1_kpzf as t1_kpzf,
                   |$bugzdf as bugzdf,
                   |$channel_position as channel_position,
                   |$initialCapital as finalCapital,
                   |sum(if(jjyk>=0,1,0)) as z,
                   |sum(if(jjyk>=0,1,0))/count(1) as zbfb,
                   |sum(if(jjyk<0,1,0)) as f,
                   |sum(if(jjyk<0,1,0))/count(1) as fbfb,
                   |count(1) as zs
                   | from ta2""".stripMargin)


              if (df11 == null) {
                df11 = middf
              } else {
                df11 = df11.union(middf)
              }

              //            println(s"最终总金额: $initialCapital")
            }
          }
//        }
              df11.orderBy(col("finalCapital").desc).show(10000)
//        df11.repartition(1).write.mode("append").parquet("file:///C:\\Users\\Administrator\\Desktop\\gs2024\\result0301")
      }
      df.unpersist()
      //    df11.orderBy(col("zbfb").desc).show(10000)
      //    df11.orderBy(col("row"),col("t1_kpzf"),col("bugzdf"),col("channel_position")).show(10000)


    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }
}
