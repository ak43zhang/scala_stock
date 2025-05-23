package sparktask.adata

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.sql.functions._

/**
 * 读取数据,
 * sfzt：是否涨停（封板）  1 0
 * cjzt：曾经涨停（炸板）  1 0
 */
object SparkReadData {
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

//    val df = spark.read.parquet("file:///D:\\gsdata\\gpsj_day_all_hs")
//      .withColumn("trade_time", date_format(col("trade_time"), "yyyy-MM-dd"))
//
//    df.createTempView("ta1")
//
//    spark.sql(
//      """select * from ta1
//        |where trade_time = '2024-12-13' and kxzt = '红柱下影线'
//        |
//        |""".stripMargin).show(1000)

//    spark.sql(
//      """select * from ta1
//        |where trade_time between '2024-12-02' and '2024-12-11'
//        |and sfzt=1
//        |""".stripMargin).show()

//    println(df.count())

//    df.filter("sfzt=1 and trade_time='2024-12-13 00:00:00' and bk in ('sz','sh')").show(200)
//
//    df.createTempView("ta1")
//    spark.sql("select *,if(high>=round(pre_close*1.1,2) and sfzt=0,1,0) as cjzt from ta1")
//      .filter("cjzt=1 and trade_time='2024-12-13 00:00:00' and bk in ('sz','sh')")
//      .show(200)



//    val df1 = spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_10days\\trade_date_month=2024-07")
//    println(df1.count)
//
//    val df2 =  spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_10days_increment\\trade_date_month=2024-07")
//    println(df2.count)

//    val df3 = spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_h10days\\trade_date_month=2024-11")
//      .where("stock_code='000031' and t0_trade_date='2024-08-01'")
//
//    val df4 =  spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_h10days_increment\\trade_date_month=2024-11")
//      .where("stock_code='603160' and t0_trade_date='2024-11-22'")
//
//    val df5 = spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_20days\\trade_date_month=2024-11")
//      .where("stock_code='603160' and t0_trade_date='2024-11-22'")
//
//    val df6 = spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_20days_increment\\trade_date_month=2024-11")
//      .where("stock_code='603160' and t0_trade_date='2024-11-22'")

//    df1.show()
//    df2.show()
//    df3.show()
//    df4.show()
//    df5.show()
//    df6.show()
//    val diff1 = df1.except(df2)
//    diff1.show()
//    val diff2 = df2.except(df1)
//    diff2.show()
//    val intersect = df1.intersect(df2)
//    println(diff1.count() == 0 && diff2.count() == 0 && intersect.count() == df1.count())


//    val df = spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_20days\\trade_date_month=2024*")
//    df.createTempView("ta1")
//    val df2 = spark.sql(
//      """
//        |select stock_code,t3_trade_date,t5_spzf,t5_close-t4_high as `当天收益`,t6_open,t6_open-t4_high as `竞价收益`
//        | from ta1 where t0_sfzt=0 and t1_sfzt=0 and t2_sfzt=0 and t3_sfzt=1 and t4_sfzt=0 and t5_high>t4_high and t5_zgzf>7
//        | order by t3_trade_date desc
//        |""".stripMargin)
//      df2.show(1000)
//    println(df2.where("`竞价收益`>0").count())
//    println(df2.where("`竞价收益`<0").count())



    val df = spark.read.parquet("file:///D:\\gsdata\\gpsj_day_all_hs\\trade_date_month=2024-12","file:///D:\\gsdata\\gpsj_day_all_hs\\trade_date_month=2025*")
    df.createTempView("ta1")


    //短线效应在于拉高后的回撤，情绪低则回撤大，回撤多
    /**
     * 核心1：此次确定赚的是当天的钱，次日竞价卖
     * 核心2：当天市场太好，第二日会有大量获利了结，则情绪变差，动手要忍
     * 核心3：权重股竞价强势，不出手【全强则例外】
     *
     *
     *
     */

    val df2 = spark.sql(
      """select trade_date,
        |sum(if(sfzt=1,1,0)) as `涨停数`,
        |sum(if(sfdt=1,1,0)) as `跌停数`,
        |round(sum(if(stzf<=0,1,0))/count(1),2) as `实体小于0比例`,
        |round(sum(if(spzf<=0,1,0))/count(1),2) as `收盘小于0比例`,
        |round(sum(if(zgzf>=5 and spzf-5>=0,1,0))/sum(if(zgzf>=5,1,0)),2) as hl,
        |round(sum(if(zgzf>=5 and spzf-5<0,1,0))/sum(if(zgzf>=5,1,0)),2) as ks,
        |round(sum(if(zgzf>=5 and zgzf-spzf>=0 and zgzf-spzf<=1,1,0))/sum(if(zgzf>=5,1,0)),2) as qx1,
        |round(sum(if(zgzf>=5 and zgzf-spzf>1 and zgzf-spzf<=2,1,0))/sum(if(zgzf>=5,1,0)),2) as qx2,
        |round(sum(if(zgzf>=5 and zgzf-spzf>2 and zgzf-spzf<=3,1,0))/sum(if(zgzf>=5,1,0)),2) as qx3,
        |round(sum(if(zgzf>=5 and zgzf-spzf>3 and zgzf-spzf<=4,1,0))/sum(if(zgzf>=5,1,0)),2) as qx4,
        |round(sum(if(zgzf>=5 and zgzf-spzf>4 and zgzf-spzf<=5,1,0))/sum(if(zgzf>=5,1,0)),2) as qx5,
        |round(sum(if(zgzf>=5 and zgzf-spzf>5,1,0))/sum(if(zgzf>=5,1,0)),2) as qx6
        |from ta1  group by trade_date order by trade_date """.stripMargin)
      df2.show(200)

      df2.createTempView("ta2")

//      spark.sql("""select a,count(1) as c  from ta2 group by a""").show()
















    val endm = System.currentTimeMillis()
    println("共耗时："+(endm-startm)/1000+"秒")
    spark.close()
  }
}
