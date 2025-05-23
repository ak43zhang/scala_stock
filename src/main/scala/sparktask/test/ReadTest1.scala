package sparktask.test

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

object ReadTest1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "10")
      .set("spark.driver.memory", "4g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "10")
      .set("spark.sql.parquet.enableVectorizedReader","false")
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

    val setdate = "2025-04-11"
    val setdate_ = setdate.replaceAll("-","")
    val year = setdate.substring(0,4)

    var basequerydf: DataFrame = spark.read.jdbc(url, s"wencaiquery_basequery_$year", properties)
      .where(s"trade_date='$setdate'")
      .select("`代码`", "`简称`")
    //      println("querydf-----------"+querydf.count())
    basequerydf.createOrReplaceTempView("basequery")

    var venturedf: DataFrame = spark.read.jdbc(url, s"wencaiquery_venture_$year", properties)
      .where(s"trade_date='$setdate'")
    //      println("querydf-----------"+querydf.count())
    venturedf.createOrReplaceTempView("venture")

    var venture_year_df: DataFrame = spark.read.jdbc(url, s"wencaiquery_venture_year", properties)
      .where(s"year='$year'")
    //      println("querydf-----------"+querydf.count())
    venture_year_df.createOrReplaceTempView("venture_year")

    spark.sql(
      """
        |select * from basequery left join (select * from
        |(select q1.`代码` as dm,collect_list(`风险类型`) as fxlxs from basequery as q1 left join (select * from venture union select * from venture_year) as q2 on q1.`代码`=q2.`代码` where `风险类型` not like '%屠龙刀%' group by q1.`代码`)
        |order by size(fxlxs) desc) as q3 on basequery.`代码`=q3.dm where q3.dm is null
        |""".stripMargin).select("代码").repartition(1).write.mode("overwrite").option("encoding", "UTF-8")
            .text(s"file:///C:\\Users\\Administrator\\Desktop\\gs2024\\filter_table\\${setdate_}\\关注")

    spark.sql(
      """
        |select * from basequery left join (select * from
        |(select q1.`代码` as dm,collect_list(`风险类型`) as fxlxs from basequery as q1 left join (select * from venture union select * from venture_year) as q2 on q1.`代码`=q2.`代码` where `风险类型` not like '%屠龙刀%' group by q1.`代码`)
        |order by size(fxlxs) desc) as q3 on basequery.`代码`=q3.dm where q3.dm is not null
        |""".stripMargin).select("代码").repartition(1).write.mode("overwrite").option("encoding", "UTF-8")
      .text(s"file:///C:\\Users\\Administrator\\Desktop\\gs2024\\filter_table\\${setdate_}\\风险")


//    val df = spark.read.parquet("file:///D:\\gsdata\\gpsj_day_all_hs\\trade_date_month=2025*")
//      df.orderBy(col("trade_time").desc).show()
//      df.createTempView("t1")

//    spark.read.parquet("file:///D:\\gsdata\\pressure_support_calculator\\valid_results_pressure_advanced_dip_strategy")
//          .where("stock_code in ('000759','002866','002717')").orderBy(col("trade_time").desc).show(100)

//    spark.read.parquet("file:///C:\\Users\\Administrator\\Desktop\\gs2024\\result0301")
//      .withColumn("finalCapital",col("finalCapital").cast(FloatType))
//      .orderBy(col("finalCapital").desc).show(1000)


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
