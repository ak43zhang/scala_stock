package sparktask.test

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

object ReadTest2 {
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

    import spark.implicits._

    val ta1: DataFrame = spark.read.jdbc(url, "tld_filter", properties)
      .where("trade_date > '2023-01-01'")
//      .where("trade_date between '2023-04-01' and '2024-09-01'")
    ta1.persist(StorageLevel.MEMORY_AND_DISK_SER)
    ta1.createTempView("ta1")
    val ta2: DataFrame = spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_20days\\trade_date_month=20[17,18,19,20,21,22,23,24,25]*")
        .select("stock_code","t0_trade_date","t0_sfzt","t0_cjzt","t1_sfzt","t1_cjzt","t0_kpzf","t0_zgzf","t0_zdzf","t0_spzf","t1_kpzf","t1_zgzf","t1_zdzf","t1_spzf")
    ta2.persist(StorageLevel.MEMORY_AND_DISK_SER)
    ta2.createOrReplaceTempView("ta2")

    val n = 1
    val bugzdf = 9.5
    val row = 1
    val tws = ArrayBuffer("asc","desc")
//    val tws = ArrayBuffer("desc")

    var df11: DataFrame = null

          val middf1 = spark.sql(
            s"""
               |select dm,`简称`,trade_date,t0_sfzt,t0_cjzt,t1_sfzt,t1_cjzt,
               |t0_kpzf,t0_zgzf,t0_zdzf,t0_spzf,
               |t1_kpzf,t1_zgzf,t1_zdzf,t1_spzf,
               |cast(t0_spzf-($bugzdf)+t1_kpzf as double) as jjyk,
               |cast(t0_spzf-($bugzdf)+t1_zgzf as double) as zgyk,
               |cast(t0_spzf-($bugzdf)+(t1_kpzf+t1_zgzf+t1_spzf+t1_zdzf)/4 as double) as pjyk,
               |cast(t0_spzf-($bugzdf)+t1_spzf as double) as spyk,
               |cast(case when t1_kpzf<=-5 then t0_spzf-$bugzdf+t1_kpzf
               |  when t1_kpzf>-5  then (t1_kpzf+t1_zgzf+t1_spzf+t1_zdzf)/4
               |  else t0_spzf-$bugzdf+t1_kpzf
               |  end as double) as zdyyk
               |  ,row_number() over(partition by trade_date order by trade_date asc,t0_kpzf desc) as row
               |from ta2 left join ta1 on ta1.dm=ta2.stock_code and ta1.trade_date=ta2.t0_trade_date
               |where dm is not null  and t0_zgzf >= $bugzdf and t0_zdzf<=$bugzdf  and t0_kpzf<=5
               |order by trade_date
               |""".stripMargin)
            .where(s"row<=$row")
            .distinct()
          middf1.orderBy("trade_date").show(10000)

          middf1.createOrReplaceTempView("ta3")


          val df3 = spark.sql("select trade_date as date,collect_list(jjyk) as jjyks,collect_list(zgyk) as zgyks,collect_list(spyk) as spyks,collect_list(pjyk) as pjyks,collect_list(zdyyk) as zdyyks from ta3 group by trade_date order by trade_date")
          df3.createOrReplaceTempView("ta4")

          df3.show(10000,false)

          // 初始本金
          var jjinitialCapital = 10000.0
          var zginitialCapital = 10000.0
          var spinitialCapital = 10000.0
          var pjinitialCapital = 10000.0
          var zdyinitialCapital = 10000.0

          // 按日期排序并收集到本地（假设数据量不大）
          val sortedDates = df3.orderBy("date")
            .select("date", "jjyks", "zgyks", "spyks", "pjyks", "zdyyks")
            .as[(String, Array[Double], Array[Double], Array[Double], Array[Double], Array[Double])]
            .collect()

          // 计算每天的累计总金额
          val results = sortedDates.map { case (date, jjchanges, zgchanges, spchanges, pjchanges, zdychanges) =>
            // 计算当天的交易影响总和
            val jjdailyChange = jjchanges.map { change =>
              (jjinitialCapital / (row*n)) * (change / 100.0) // 每笔交易的影响
            }.sum
            // 计算当天的交易影响总和
            val zgdailyChange = zgchanges.map { change =>
              (zginitialCapital / (row*n)) * (change / 100.0)
            }.sum
            // 计算当天的交易影响总和
            val spdailyChange = spchanges.map { change =>
              (spinitialCapital / (row*n)) * (change / 100.0)
            }.sum
            val pjdailyChange = pjchanges.map { change =>
              (pjinitialCapital / (row*n)) * (change / 100.0)
            }.sum
            // 计算当天的交易影响总和
            val zdydailyChange = zdychanges.map { change =>
              (zdyinitialCapital / (row*n)) * (change / 100.0)
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
          resultDF.orderBy("date").show(10000, false)


          val middf = spark.sql(
            s"""select
               |$bugzdf as bugzdf,
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
               | from ta3""".stripMargin)

//          middf.show()

          if (df11 == null) {
            df11 = middf
          } else {
            df11 = df11.union(middf)
          }



    df11.orderBy(col("spinitialCapital").desc).show(10000)

    //    var df: DataFrame = spark.read.jdbc(url, "data_jyrl", properties)
    //    df.createTempView("data_jyrl")
    //    val listsetDate = spark.sql("select trade_date from data_jyrl where  trade_date between '2018-01-01' and '2025-01-11' and trade_status='1' order by trade_date desc")
    //      .collect()
    //      .map(f => f.getAs[String]("trade_date").replaceAll("-", ""))
    //
    //    for(setdate<-listsetDate){
    //      if(spark.read.jdbc(url, s"wencaiquery_venture_$setdate", properties).where("`风险类型`='流动性风险_换手2'").count()==0){
    //        println(setdate)
    //      }
    //    }


    //    var df: DataFrame = spark.read.jdbc(url, "data_jyrl", properties)
    //        df.createTempView("ta1")
    //
    //    var ztbdf:DataFrame = spark.read.jdbc(url, s"ztb_day", properties)
    //    ztbdf.select("trade_date").distinct().createTempView("ta2")
    //
    //    spark.sql(
    //      """select ta1.*,ta2.* from ta1 left join ta2 on ta1.trade_date=ta2.trade_date
    //        |where trade_status=1 and ta1.trade_date between '2018-01-01' and '2025-03-04'  and ta2.trade_date is null
    //        |order by ta1.trade_date desc
    //        |""".stripMargin).show(100)


    //    val df3 = spark.read.parquet("file:///D:\\gsdata\\pressure_support_calculator\\valid_results_pressure_advanced_dip_strategy")
    //    df3.select("stock_code","trade_time","support_ratio","pressure_ratio","channel_position","windowSize")
    //      .where("trade_time='2025-03-03' and stock_code in('603278','601177','605208','603583','600592','600835')")
    //      .orderBy("support_ratio").show(3000)

    //    val df = spark.read.jdbc(url, "pressure_support_calculator2020", properties)
    //      .where(s"trade_date between '2018-01-01' and '2019-01-01'")
    //      .write.mode("append").jdbc(url, "pressure_support_calculator2018", properties)
    //    df.createTempView("ta1")
    //    df.orderBy("trade_time").show(20)
    //
    //    val bugzdf = 5
    //    val df2 = spark.sql(
    //     s"""
    //        |select stock_code,trade_time,channel_position,if(t1_spzf-$bugzdf+t2_kpzf>=0,1,0) as jjsign,if(t1_spzf-$bugzdf+t2_spzf>=0,1,0) as spsign
    //        | from ta1
    //        | order by trade_time
    //        |""".stripMargin)
    //    df2.show()
    //      df2.createTempView("ta2")
    //
    //    val df3 = spark.sql(
    //      """
    //        |select trade_time,sum(jjsign) as s1,count(1) as c1,sum(jjsign)/count(1) as sl,
    //        |sum(if(channel_position>=10 and channel_position <40,jjsign,0)) as s2,
    //        |sum(if(channel_position>=10 and channel_position <40,1,0)) as c2,
    //        |sum(if(channel_position>=10 and channel_position <40,jjsign,0))/sum(if(channel_position>=10 and channel_position <40,1,0)) as sl2
    //        | from ta2
    //        |group by trade_time
    //        |order by trade_time
    //        |""".stripMargin)
    //      df3.show(414)
    //    df3.createTempView("ta3")
    //
    //    spark.sql(
    //      """
    //        |select sum(if(sl2>sl,1,0)) from ta3
    //        |""".stripMargin).show()

    //    val df = spark.read.parquet("file:///D:\\gsdata\\gpsj_day_all_hs\\trade_date_month=202*")
    //      df.orderBy(col("trade_time").desc).show()
    //      df.createTempView("t1")

    //    val df3 = spark.read.parquet("file:///D:\\gsdata\\pressure_support_calculator\\valid_results_pressure_advanced_dip_strategy")
    //    df3.select("trade_time").distinct().orderBy("trade_time").where("trade_time between '2022-01-01' and '2023-01-01'").show(300)
    //
    //    var df:DataFrame = spark.read.jdbc(url, "data_jyrl", properties)
    //    df.createTempView("data_jyrl")
    //    spark.sql("select trade_date from data_jyrl where  trade_date between '2022-01-01' and '2023-01-01' and trade_status='1' order by trade_date ").show(1000)


    /*    val df3 = spark.read.parquet("file:///D:\\gsdata\\pressure_support_calculator\\valid_results_pressure_advanced_dip_strategy")
    //      .where("trade_time like '2025%'")
    //    df3.where("stock_code='002370' and trade_time like '2025-02%'").show(100)
        df3.createTempView("ta1")



        val df = spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_10days\\trade_date_month=202*")

    //    df.printSchema()
    //          df.show()
              df.createTempView("ta2")

        spark.sql(
          """
            |select * ,FLOOR(channel_position/10) as z,SUBSTR(trade_time, 1, 4) as y,t1_spzf-5 as spyk from ta1 left join ta2 on ta1.stock_code=ta2.stock_code and ta1.trade_time=ta2.t0_trade_date
            |""".stripMargin).show(10)

        for(buy<-3 to 10){
          spark.sql(
            s"""
               |select z,y,sum(if(spyk>=0,1,0)) as yl,sum(if(spyk<0,1,0)) as ks,sum(if(spyk>=0,1,0))/count(1) as ylbfb  from
               |(select FLOOR(channel_position/10) as z,SUBSTR(trade_time, 1, 4) as y,t1_spzf-$buy as spyk from ta1 left join ta2 on ta1.stock_code=ta2.stock_code and ta1.trade_time=ta2.t0_trade_date
               |where t1_zgzf>=$buy and t1_kpzf>$buy)
               |group by z,y
               |order by z,y
               |""".stripMargin).show(1000)

        }*/


    //        spark.sql(
    //          """
    //            |select kxzt,trade_date,count(1)  from t1
    //            |group by trade_date,kxzt
    //            |order by trade_date,kxzt
    //            |""".stripMargin)
    //      .show()
    //          .createTempView("t2")


    //    spark.sql(
    //      """
    //        |select trade_date,sum(if(kpzf>0,1,0)) as yl,sum(if(kpzf>0,1,0))/count(1) as ykb from ta1
    //        |group by trade_date
    //        |order by trade_date
    //        |""".stripMargin).where("ykb>=0.5").createTempView("t2")


    //    val setdate = "20250225"
    //    val formattedDate = s"${setdate.take(4)}-${setdate.slice(4, 6)}-${setdate.takeRight(2)}"
    //
    //    var df:DataFrame = spark.read.jdbc(url, s"wencaiquery_basequery_$setdate", properties).select("`代码`","`简称`").toDF("dm","jc")
    //    df.createTempView("ta1")
    //
    //    var df2:DataFrame = spark.read.jdbc(url, s"wencaiquery_venture_$setdate", properties).toDF("dm","jc","fxlx")
    //    df2.createTempView("ta2")
    //
    //    val df3 = spark.read.parquet("file:///D:\\gsdata\\pressure_support_calculator\\valid_results_pressure_advanced_dip_strategy")
    //    df3.createTempView("ta3")
    //
    //    spark.sql(
    //      """
    //        |select ta1.dm,ta3.trade_time,windowSize,channel_position,support_types,pressure_types from ta1 left join ta2 on ta1.dm=ta2.dm left join ta3 on ta1.dm=ta3.stock_code where trade_time='2025-02-24'
    //        |and fxlx is null order by channel_position desc
    //        |""".stripMargin).show(1000)


    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }
}
