package sparktask.test

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
 * 固定参数回测验证函数
 */
object ReadParquetP2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.scheduler.listenerbus.eventqueue.capacity", "100000")
      .set("spark.scheduler.listenerBus.eventQueue.size", "100000")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.driver.memory", "16g")
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

    val df = spark.read.jdbc(url, "fx002_v5", properties)
//            .where(s"t1_trade_date between '2023-01-01' and '2024-09-15'")
//            .where(s"t1_trade_date not between '2022-03-01' and '2023-11-01'")
      //            .where(s"t1_trade_date not between '2023-04-01' and '2024-01-01' and t1_trade_date not between '2024-05-01' and '2024-09-01'")
      //      .where("support_ratio<130")
      .distinct()
    df.createOrReplaceTempView("ta1")

//    val tdf = spark.read.parquet("file:///D:\\gsdata\\gpsj_day_all_hs\\trade_date_month=202*")
//    tdf.createTempView("t1")
//
//    spark.sql(
//      """
//        |select trade_date,sum(if(kpzf>0,1,0)) as yl,sum(if(kpzf>=0,1,0))/count(1) as ykb from t1
//        |group by trade_date
//        |order by trade_date
//        |""".stripMargin)
//      .where("ykb>=0.2")
//      .createTempView("t2")
//
//    spark.sql("select m1.* from m1 left join t2 on m1.t1_trade_date=t2.trade_date where t2.trade_date is not null").createTempView("ta1")

    val bugzdf = 8
    val row = 1
    //val channel_position = 40

    /**
     * bugzdf           购买涨跌幅
     * channel_position 通道位置
     * t1_kpzf          有限购买开盘涨幅大的
     * tw               desc asc
     *
     */
    var df11: DataFrame = null
//          for (row <- 1 to 1) {
//            for (channel_position <- 40 to 40 by 10) {
//              for (bugzdf <- 4 to 4) {

    val df2 = spark.sql(
      s"""
         |select * from
         |(select stock_code,trade_time,windowSize,close,composite_support,composite_pressure,support_ratio,pressure_ratio,channel_position,t1_trade_date,t1_sfzt,t1_cjzt,
         |t1_kpzf,t1_zgzf,t1_spzf,t2_kpzf,t2_zgzf,t2_zdzf,t2_spzf,
         |cast(t1_spzf-$bugzdf+t2_kpzf as double) as jjyk,
         |cast(t1_spzf-$bugzdf+t2_zgzf as double) as zgyk,
         |cast(t1_spzf-$bugzdf+(t2_kpzf+t2_zgzf+t2_spzf+t2_zdzf)/4 as double) as pjyk,
         |cast(t1_spzf-$bugzdf+t2_spzf as double) as spyk,
         |cast(case when t2_kpzf<-2 then t1_spzf-$bugzdf+t2_kpzf
         |  when t2_kpzf>-2 then (t2_kpzf+t2_zgzf+t2_spzf+t2_zdzf)/4
         |  else t1_spzf-$bugzdf+t2_kpzf
         |  end as double) as zdyyk
         |,row_number() over(partition by t1_trade_date order by t1_trade_date asc,t1_kpzf desc) as row from ta1
         |where  t1_zgzf>=$bugzdf and $bugzdf>=t1_kpzf  and t1_kpzf>=-2 and t1_kpzf<4 and t1_zgzf>=8 )
         | where row<=$row
         |order by trade_time
         |""".stripMargin)
        // and channel_position <40

    df2.orderBy(col("trade_time")).show(4000)
    df2.createOrReplaceTempView("ta2")


    val df3 = spark.sql("select t1_trade_date as date,collect_list(jjyk) as jjyks,collect_list(zgyk) as zgyks,collect_list(spyk) as spyks,collect_list(pjyk) as pjyks,collect_list(zdyyk) as zdyyks from ta2 group by t1_trade_date order by t1_trade_date")
    df3.createOrReplaceTempView("ta3")
    //      df3.show(false)

    // 初始本金
    var jjinitialCapital = 10000.0
    var zginitialCapital = 10000.0
    var spinitialCapital = 10000.0
    var pjinitialCapital = 10000.0
    var zdyinitialCapital = 10000.0

    // 按日期排序并收集到本地（假设数据量不大）
    val sortedDates = df3.orderBy("date")
      .select("date", "jjyks", "zgyks", "spyks","pjyks", "zdyyks")
      .as[(String,  Array[Double],Array[Double], Array[Double], Array[Double], Array[Double])]
      .collect()

    // 计算每天的累计总金额
    val results = sortedDates.map { case (date, jjchanges, zgchanges, spchanges, pjchanges, zdychanges) =>
      // 计算当天的交易影响总和
      val jjdailyChange = jjchanges.map { change =>
        (jjinitialCapital / (row)) * (change / 100.0) // 每笔交易的影响
      }.sum
      // 计算当天的交易影响总和
      val zgdailyChange = zgchanges.map { change =>
        (zginitialCapital / (row)) * (change / 100.0) // 每笔交易的影响
      }.sum
      // 计算当天的交易影响总和
      val spdailyChange = spchanges.map { change =>
        (spinitialCapital / (row)) * (change / 100.0) // 每笔交易的影响
      }.sum
      val pjdailyChange = pjchanges.map { change =>
        (pjinitialCapital / (row)) * (change / 100.0) // 每笔交易的影响
      }.sum
      // 计算当天的交易影响总和
      val zdydailyChange = zdychanges.map { change =>
        (zdyinitialCapital / (row)) * (change / 100.0) // 每笔交易的影响
      }.sum
      // 更新总金额
      jjinitialCapital += jjdailyChange
      zginitialCapital += zgdailyChange
      spinitialCapital += spdailyChange
      pjinitialCapital += pjdailyChange
      zdyinitialCapital += zdydailyChange
      (date, jjdailyChange, jjinitialCapital, zgdailyChange, zginitialCapital, spdailyChange, spinitialCapital, pjdailyChange, pjinitialCapital, zdydailyChange, zdyinitialCapital)
    }

    // 转换为 DataFrame 输出
    val resultDF = spark.createDataFrame(results).toDF("date", "jjdailyChange", "jjinitialCapital", "zgdailyChange", "zginitialCapital", "spdailyChange", "spinitialCapital", "pjdailyChange", "pjinitialCapital", "zdydailyChange", "zdyinitialCapital")
    resultDF.orderBy("date").show(1000, false)


    val middf = spark.sql(
      s"""select
         |$row as row,
         |'1-4' as t1_kpzf,
         |$bugzdf as bugzdf,
         |'10-40' as channel_position,
         |round($jjinitialCapital,2) as jjinitialCapital,
         |round($zginitialCapital,2) as zginitialCapital,
         |round($spinitialCapital,2) as spinitialCapital,
         |round($pjinitialCapital,2) as pjinitialCapital,
         |round($zdyinitialCapital,2) as zdyinitialCapital,
         |sum(if(jjyk>=0,1,0)) as jjyl,
         |round(sum(if(jjyk>=0,1,0))/count(1),2) as jjylbfb,
         |sum(if(jjyk<0,1,0)) as jjks,
         |round(sum(if(jjyk<0,1,0))/count(1),2) as jjksbfb,
         |
         |sum(if(zgyk>=0,1,0)) as zgyl,
         |round(sum(if(zgyk>=0,1,0))/count(1),2) as zgylbfb,
         |sum(if(zgyk<0,1,0)) as zgks,
         |round(sum(if(zgyk<0,1,0))/count(1),2) as zgksbfb,
         |
         |sum(if(spyk>=0,1,0)) as spyl,
         |round(sum(if(spyk>=0,1,0))/count(1),2) as spylbfb,
         |sum(if(spyk<0,1,0)) as spks,
         |round(sum(if(spyk<0,1,0))/count(1),2) as spksbfb,
         |
         |sum(if(pjyk>=0,1,0)) as pjyl,
         |round(sum(if(pjyk>=0,1,0))/count(1),2) as pjylbfb,
         |sum(if(pjyk<0,1,0)) as pjks,
         |round(sum(if(pjyk<0,1,0))/count(1),2) as pjksbfb,
         |
         |sum(if(zdyyk>=0,1,0)) as zdyyl,
         |round(sum(if(zdyyk>=0,1,0))/count(1),2) as zdyylbfb,
         |sum(if(zdyyk<0,1,0)) as zdyks,
         |round(sum(if(zdyyk<0,1,0))/count(1),2) as zdyksbfb,
         |count(1) as zs
         | from ta2""".stripMargin)


    if (df11 == null) {
      df11 = middf
    } else {
      df11 = df11.union(middf)
    }

    //    println(s"最终总金额: $initialCapital")
//              }
//            }
//          }
    df11.orderBy(col("spinitialCapital").desc).show(10000)
    //        df11.repartition(1).write.mode("append").parquet("file:///C:\\Users\\Administrator\\Desktop\\gs2024\\result8")


    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }
}
