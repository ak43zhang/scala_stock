package sparktask.mysql.update.demotest

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 用于测试
 */
object Demo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.local.dir","D:\\Temp")
    val spark = SparkSession
      .builder()
      .appName("Demo1")
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    //#################################################################################
    for(i<-2000 to 2024){
      val df1 = spark.read.parquet(s"file:///D:\\gsdata\\gpsj_15days\\trade_date_month=$i*")
      df1.createOrReplaceTempView("ta1")
      println(i)
      zf1_zp(spark)
    }

//    val df1 = spark.read.parquet(s"file:///D:\\gsdata\\gpsj_15days\\trade_date_month=2024-03")
//          df1.createOrReplaceTempView("ta1")
//          zf1_zp(spark)
//          zf2_zp(spark)
    //#################################################################################




//    println(spark.read.parquet("file:///D:\\gsdata\\gpsj_day").count())
//    println(spark.read.parquet("file:///D:\\gsdata\\gpsj_day").distinct().count())

//    spark.read.parquet("file:///D:\\gsdata\\gpsj_day").printSchema()

//    spark.read.parquet("file:///D:\\gsdata\\gpsj_day\\trade_date_month=2024-05_1").distinct().repartition(4).write.mode("overwrite")
//      .parquet("file:///D:\\gsdata\\gpsj_day\\trade_date_month=2024-05")

//    spark.sql("select round(4.86*1.1,2) as a").show()


    //((t1_change_pct>9.9 and t1_ln in('放量','放巨量')) or (t2_change_pct>9.9 and t2_ln in('放量','放巨量')) or (t3_change_pct>9.9 and t3_ln in('放量','放巨量')) ) and
//    spark.read.parquet("file:///D:\\gsdata\\gpsj_9days_month\\trade_date_month=2024-06")
//      .where(
//        """t7_zt='红柱下影线' and
//          |((t1_change_pct>9.9 and t1_ln in('放巨量')) or (t2_change_pct>9.9 and t2_ln in('放巨量')) or (t3_change_pct>9.9 and t3_ln in('放巨量')) ) and
//          |t4_ln='缩量' and t5_ln='缩量' and t6_ln='缩量'
//          |""".stripMargin)
//      .select("stock_code","t7_close","t8_trade_date","t8_open","t8_close","t8_high","t8_low","t8_volume","t8_amount","t8_pre_close","t8_kp","t8_wp","t8_sx","t8_xx","t8_ln","t8_zt","t8_turnover_ratio","t8_change","t8_change_pct")
//      .show(100,false)


    //1、首板，3连阴缩量,3连阴跌幅超过百分之7，出现微小线放量
//    spark.sql(
//      """select
//        | stock_code,t0_trade_date,t3_trade_date,t8_change_pct,t9_change_pct,t10_change_pct
//        | from ta1 where t2_sfzt=0 and t3_sfzt=1
//        | and t5_ln='缩量'
//        | and t6_ln='缩量'
//        | and t7_zt='红柱下影线' and t7_ln='放量' and t4_change_pct+t5_change_pct+t6_change_pct<-7
//        |""".stripMargin).show(false)

//    spark.sql("select count(1),t3_trade_date from ta1 where t3_sfzt=1 group by t3_trade_date").show(300)

//    spark.sql(
//      """select
//        | stock_code,t0_trade_date,t3_trade_date,t8_change_pct,t9_change_pct,t10_change_pct,t11_change_pct
//        | from ta1 where t2_sfzt=0 and t3_sfzt=1
//        | and t5_ln='缩量'
//        | and t6_ln='缩量'
//        | and t7_ln='缩量'
//        | and t8_zt='红柱下影线' and t8_ln='放量' and t4_change_pct+t5_change_pct+t6_change_pct+t7_change_pct<-7
//        |""".stripMargin).show(false)


    spark.close()
  }

  /**
   * 战法1：首板价格回归战法——（回测）
   *
   * t3首板涨停
   * t4量能小于t3量能的3倍
   * t4,5,6,7,8收盘价均小于t3涨停价
   * t4,5,6,7,8总跌幅超过百分之7
   * t9为微笑线
   *
   *
   * 阶段高点不买（200日）
   * 大周期内不买
   * 高点不做多
   * M头不买
   *
   */
  def zf1_hc(spark:SparkSession): Unit ={
    import spark.implicits._
    val res = spark.sql(
      """select
        | stock_code,t0_trade_date,t3_trade_date,t9_trade_date,t4_volume,t8_zt,t4_change_pct+t5_change_pct+t6_change_pct+t7_change_pct+t8_change_pct as t4_8,t9_change_pct,t10_change_pct,t10_change_pct+t11_change_pct as  t1011,t10_change_pct+t11_change_pct+t12_change_pct as t1012
        |from ta1 where t2_sfzt=0 and t3_sfzt=1 and t4_sfzt=0 and t4_volume/t3_volume<3
        | and t4_close<t3_close and t5_close<t3_close and t6_close<t3_close and t7_close<t3_close and t8_close<t3_close and t9_zt='红柱下影线'
        | and t4_change_pct+t5_change_pct+t6_change_pct+t7_change_pct+t8_change_pct<-7
        |order by t0_trade_date
        |""".stripMargin)
    res.createOrReplaceTempView("ta2")
    //    res.show(100)

    val df2 = spark.sql(
      """select *,if(t10_change_pct>1.5,t10_change_pct,if(t10_change_pct<-4,-4,if(t1011>-1,t1011,if(t1011<-2,-2,t1012)))) as q,
        |(1+if(t10_change_pct>1.5,t10_change_pct,if(t10_change_pct<-4,-4,if(t1011>-1,t1011,if(t1011<-2,-2,t1012))))/100) as q2
        |from ta2""".stripMargin).where("q is not null and t4_8>-30 and q2>0.9 and q2<1.1")
    df2.show(100)
    println(df2.count())

    println(df2.select("q2").as[BigDecimal].collect().reduce(_ * _))
  }

  /**
   * 战法1：首板价格回归战法（找票）
   *
   * t3首板涨停
   * t4量能小于t3量能的3倍
   * t4,5,6,7,8收盘价均小于t3涨停价
   * t4,5,6,7,8总跌幅超过百分之7
   * --t9为微笑线
   * 价格在3元以上，60以下
   *
   *
   * 阶段高点不买（200日）
   * 大周期内不买
   * 高点不做多
   * M头不买
   *
   */
  def zf1_zp(spark:SparkSession): Unit ={
    import spark.implicits._
    val res = spark.sql(
      """select
        | stock_code,t0_trade_date,t3_trade_date,t9_trade_date,t4_volume,t8_zt,t9_zt,t4_change_pct+t5_change_pct+t6_change_pct+t7_change_pct+t8_change_pct as t4_8,t9_change_pct,t10_change_pct,t10_change_pct+t11_change_pct as  t1011,t10_change_pct+t11_change_pct+t12_change_pct as t1012
        |from ta1 where
        | t0_sfzt=0 and t1_sfzt=0 and t2_sfzt=0 and t3_sfzt=1 and t4_sfzt=0
        | and t4_volume/t3_volume<3
        | and t4_close<t3_close and t5_close<t3_close and t6_close<t3_close and t7_close<t3_close and t8_close<t3_close
        | and t4_change_pct+t5_change_pct+t6_change_pct+t7_change_pct+t8_change_pct<-7
        | and t8_open>=3 and t8_open <60
        | and (t9_zt!='红柱上影线' and t8_zt!='%绿色上影线%')
        |order by t0_trade_date
        |""".stripMargin)
    res.createOrReplaceTempView("ta2")
        res.orderBy($"t0_trade_date".desc).show(100)

    val df2 = spark.sql(
      """select *,if(t10_change_pct>1.5,t10_change_pct,if(t10_change_pct<-4,-4,if(t1011>-1,t1011,if(t1011<-2,-2,t1012)))) as q,
        |(1+if(t10_change_pct>1.5,t10_change_pct,if(t10_change_pct<-4,-4,if(t1011>-1,t1011,if(t1011<-2,-2,t1012))))/100) as q2
        |from ta2""".stripMargin).where("q is not null and t4_8>-30 and q2>0.9 and q2<1.1 ")
    df2.orderBy($"t9_trade_date").show(500)
    df2.createTempView("df2")
    spark.sql("select q2s,r,count(1) from (select if(q2<1,-1,1) as q2s,round(t9_change_pct,0) as r from df2) group by q2s,r order by q2s,r").show(300)
    println(df2.count())

    println(df2.select("q2").as[BigDecimal].collect().reduce(_ * _))



  }

  /**
   * 战法2：首板价格回归战法2（找票）
   *
   * t3首板涨停
   * t4量能小于t3量能的3倍
   * t4,5,6,7,8收盘价均小于t3涨停价
   * t4,5,6,7,8总跌幅超过百分之7
   * t9为红柱，t10大绿柱
   * 价格在3元以上，60以下
   *
   *
   * 阶段高点不买（200日）
   * 大周期内不买
   * 高点不做多
   * M头不买
   *
   */
  def zf2_zp(spark:SparkSession): Unit ={
    import spark.implicits._
    val res = spark.sql(
      """select
        | stock_code,t0_trade_date,t3_trade_date,t9_trade_date,t4_volume,t8_zt,t9_zt,t10_zt,
        | t4_change_pct+t5_change_pct+t6_change_pct+t7_change_pct+t8_change_pct as t4_8,
        | t9_change_pct,t10_change_pct,t11_change_pct,
        | t11_change_pct+t12_change_pct as  t1112,t11_change_pct+t12_change_pct+t13_change_pct as t1113
        |from ta1 where
        | t0_sfzt=0 and t1_sfzt=0 and t2_sfzt=0 and t3_sfzt=1 and t4_sfzt=0
        | and t4_volume/t3_volume<3
        | and t4_close<t3_close and t5_close<t3_close and t6_close<t3_close and t7_close<t3_close and t8_close<t3_close
        | and t4_change_pct<2 and t5_change_pct<2 and t6_change_pct<2 and t7_change_pct<2 and t8_change_pct<2
        | and t4_change_pct+t5_change_pct+t6_change_pct+t7_change_pct+t8_change_pct<-7
        | and t8_open>=3 and t8_open <60
        | and t9_zt like '%红柱%' and t10_zt like '%绿柱%' and abs(t10_change_pct)>abs(t9_change_pct)
        |order by t0_trade_date
        |""".stripMargin)
    res.createOrReplaceTempView("ta2")
    res.orderBy($"t0_trade_date".desc).show(100)

    val df2 = spark.sql(
      """select *,if(t11_change_pct>1.5,t11_change_pct,if(t11_change_pct<-4,-4,if(t1112>-1,t1112,if(t1112<-2,-2,t1113)))) as q,
        |(1+if(t11_change_pct>1.5,t11_change_pct,if(t11_change_pct<-4,-4,if(t1112>-1,t1112,if(t1112<-2,-2,t1113))))/100) as q2
        |from ta2""".stripMargin).where("q is not null and t4_8>-30 and q2>0.9 and q2<1.1 ")
    df2.orderBy($"t9_trade_date").show(500)
    df2.createTempView("df2")
    spark.sql("select q2s,r,count(1) from (select if(q2<1,-1,1) as q2s,round(t9_change_pct,0) as r from df2) group by q2s,r order by q2s,r").show(300)
    println(df2.count())

    println(df2.select("q2").as[BigDecimal].collect().reduce(_ * _))



  }
}
