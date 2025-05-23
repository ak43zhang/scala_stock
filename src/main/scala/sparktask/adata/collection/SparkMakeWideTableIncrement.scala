package sparktask.adata.collection

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.{DataFrame, SparkSession}
import sparktask.tools.FileTools

/**
 * 制作宽表,增量宽表
 */
object SparkMakeWideTableIncrement {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions","10")
      .set("spark.driver.memory","4g")
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

    //制作增量宽表的路径
    val months = "2025-03,2025-04,2025-05"
    val m1 =months.split(",")(0)
    val m2 = months.split(",")(1)
    val m3 = months.split(",")(2)


    val df2 = spark.read.parquet(s"file:///D:\\gsdata\\gpsj_day_all_hs\\trade_date_month=$m1",
      s"file:///D:\\gsdata\\gpsj_day_all_hs\\trade_date_month=$m2",
      s"file:///D:\\gsdata\\gpsj_day_all_hs\\trade_date_month=$m3")

    df2.cache()
    df2.createTempView("ta2")

    val df3 = spark.sql("select *,row_number() over(partition by stock_code order by trade_date,stock_code) as row from ta2 ")
    df3.cache()
    df3.createTempView("ta3")

    val df4 = spark.sql(
      """
        |select t2.*,
        |if(t2.high<t1.high,'下探',if(t2.high=t1.high,'平整','突破')) as sx,
        |if(t2.low<t1.low,'下探',if(t2.low=t1.low,'平整','突破')) as xx,
        |if(t2.volume<t1.volume,'缩量',if(t2.volume=t1.volume,'平量',if(t2.volume/t1.volume>2,'放巨量','放量'))) as ln,
        |round(t2.volume/t1.volume,2) as zrlnb
        |from ta3 as t1 left join ta3 as t2 on t1.row+1=t2.row and t1.stock_code=t2.stock_code
        |where t2.row is not null """.stripMargin)

    df4.cache()
    df4.createTempView("ta4")
//    df4.show()

    //
    val df5 = spark.sql(
      """|select t0.stock_code,substring(t0.trade_date,0,7) as trade_date_month,
         |t0.trade_date as t0_trade_date,t0.bk as t0_bk,t0.sfzt as t0_sfzt,t0.cjzt as t0_cjzt, t0.open as t0_open , t0.close as t0_close ,t0.high as t0_high,t0.low as t0_low,t0.kpzf as t0_kpzf,t0.spzf as t0_spzf,t0.zgzf as t0_zgzf,t0.zdzf as t0_zdzf,t0.volume as t0_volume,t0.amount as t0_amount,t0.pre_close as t0_pre_close,t0.qjzf as t0_qjzf,t0.stzf as t0_stzf,t0.kxzt as t0_kxzt,t0.kp as t0_kp,t0.wp as t0_wp ,t0.sx as t0_sx,t0.xx as t0_xx,t0.ln as t0_ln,t0.zrlnb as t0_zrlnb,t0.turnover_ratio as t0_turnover_ratio,t0.change as t0_change,t0.change_pct as t0_change_pct,
         |t1.trade_date as t1_trade_date,t1.bk as t1_bk,t1.sfzt as t1_sfzt,t1.cjzt as t1_cjzt, t1.open as t1_open , t1.close as t1_close ,t1.high as t1_high,t1.low as t1_low,t1.kpzf as t1_kpzf,t1.spzf as t1_spzf,t1.zgzf as t1_zgzf,t1.zdzf as t1_zdzf,t1.volume as t1_volume,t1.amount as t1_amount,t1.pre_close as t1_pre_close,t1.qjzf as t1_qjzf,t1.stzf as t1_stzf,t1.kxzt as t1_kxzt,t1.kp as t1_kp,t1.wp as t1_wp ,t1.sx as t1_sx,t1.xx as t1_xx,t1.ln as t1_ln,t1.zrlnb as t1_zrlnb,t1.turnover_ratio as t1_turnover_ratio,t1.change as t1_change,t1.change_pct as t1_change_pct,
         |t2.trade_date as t2_trade_date,t2.bk as t2_bk,t2.sfzt as t2_sfzt,t2.cjzt as t2_cjzt, t2.open as t2_open , t2.close as t2_close ,t2.high as t2_high,t2.low as t2_low,t2.kpzf as t2_kpzf,t2.spzf as t2_spzf,t2.zgzf as t2_zgzf,t2.zdzf as t2_zdzf,t2.volume as t2_volume,t2.amount as t2_amount,t2.pre_close as t2_pre_close,t2.qjzf as t2_qjzf,t2.stzf as t2_stzf,t2.kxzt as t2_kxzt,t2.kp as t2_kp,t2.wp as t2_wp ,t2.sx as t2_sx,t2.xx as t2_xx,t2.ln as t2_ln,t2.zrlnb as t2_zrlnb,t2.turnover_ratio as t2_turnover_ratio,t2.change as t2_change,t2.change_pct as t2_change_pct,
         |t3.trade_date as t3_trade_date,t3.bk as t3_bk,t3.sfzt as t3_sfzt,t3.cjzt as t3_cjzt, t3.open as t3_open , t3.close as t3_close ,t3.high as t3_high,t3.low as t3_low,t3.kpzf as t3_kpzf,t3.spzf as t3_spzf,t3.zgzf as t3_zgzf,t3.zdzf as t3_zdzf,t3.volume as t3_volume,t3.amount as t3_amount,t3.pre_close as t3_pre_close,t3.qjzf as t3_qjzf,t3.stzf as t3_stzf,t3.kxzt as t3_kxzt,t3.kp as t3_kp,t3.wp as t3_wp ,t3.sx as t3_sx,t3.xx as t3_xx,t3.ln as t3_ln,t3.zrlnb as t3_zrlnb,t3.turnover_ratio as t3_turnover_ratio,t3.change as t3_change,t3.change_pct as t3_change_pct,
         |t4.trade_date as t4_trade_date,t4.bk as t4_bk,t4.sfzt as t4_sfzt,t4.cjzt as t4_cjzt, t4.open as t4_open , t4.close as t4_close ,t4.high as t4_high,t4.low as t4_low,t4.kpzf as t4_kpzf,t4.spzf as t4_spzf,t4.zgzf as t4_zgzf,t4.zdzf as t4_zdzf,t4.volume as t4_volume,t4.amount as t4_amount,t4.pre_close as t4_pre_close,t4.qjzf as t4_qjzf,t4.stzf as t4_stzf,t4.kxzt as t4_kxzt,t4.kp as t4_kp,t4.wp as t4_wp ,t4.sx as t4_sx,t4.xx as t4_xx,t4.ln as t4_ln,t4.zrlnb as t4_zrlnb,t4.turnover_ratio as t4_turnover_ratio,t4.change as t4_change,t4.change_pct as t4_change_pct,
         |t5.trade_date as t5_trade_date,t5.bk as t5_bk,t5.sfzt as t5_sfzt,t5.cjzt as t5_cjzt, t5.open as t5_open , t5.close as t5_close ,t5.high as t5_high,t5.low as t5_low,t5.kpzf as t5_kpzf,t5.spzf as t5_spzf,t5.zgzf as t5_zgzf,t5.zdzf as t5_zdzf,t5.volume as t5_volume,t5.amount as t5_amount,t5.pre_close as t5_pre_close,t5.qjzf as t5_qjzf,t5.stzf as t5_stzf,t5.kxzt as t5_kxzt,t5.kp as t5_kp,t5.wp as t5_wp ,t5.sx as t5_sx,t5.xx as t5_xx,t5.ln as t5_ln,t5.zrlnb as t5_zrlnb,t5.turnover_ratio as t5_turnover_ratio,t5.change as t5_change,t5.change_pct as t5_change_pct,
         |t6.trade_date as t6_trade_date,t6.bk as t6_bk,t6.sfzt as t6_sfzt,t6.cjzt as t6_cjzt, t6.open as t6_open , t6.close as t6_close ,t6.high as t6_high,t6.low as t6_low,t6.kpzf as t6_kpzf,t6.spzf as t6_spzf,t6.zgzf as t6_zgzf,t6.zdzf as t6_zdzf,t6.volume as t6_volume,t6.amount as t6_amount,t6.pre_close as t6_pre_close,t6.qjzf as t6_qjzf,t6.stzf as t6_stzf,t6.kxzt as t6_kxzt,t6.kp as t6_kp,t6.wp as t6_wp ,t6.sx as t6_sx,t6.xx as t6_xx,t6.ln as t6_ln,t6.zrlnb as t6_zrlnb,t6.turnover_ratio as t6_turnover_ratio,t6.change as t6_change,t6.change_pct as t6_change_pct,
         |t7.trade_date as t7_trade_date,t7.bk as t7_bk,t7.sfzt as t7_sfzt,t7.cjzt as t7_cjzt, t7.open as t7_open , t7.close as t7_close ,t7.high as t7_high,t7.low as t7_low,t7.kpzf as t7_kpzf,t7.spzf as t7_spzf,t7.zgzf as t7_zgzf,t7.zdzf as t7_zdzf,t7.volume as t7_volume,t7.amount as t7_amount,t7.pre_close as t7_pre_close,t7.qjzf as t7_qjzf,t7.stzf as t7_stzf,t7.kxzt as t7_kxzt,t7.kp as t7_kp,t7.wp as t7_wp ,t7.sx as t7_sx,t7.xx as t7_xx,t7.ln as t7_ln,t7.zrlnb as t7_zrlnb,t7.turnover_ratio as t7_turnover_ratio,t7.change as t7_change,t7.change_pct as t7_change_pct,
         |t8.trade_date as t8_trade_date,t8.bk as t8_bk,t8.sfzt as t8_sfzt,t8.cjzt as t8_cjzt, t8.open as t8_open , t8.close as t8_close ,t8.high as t8_high,t8.low as t8_low,t8.kpzf as t8_kpzf,t8.spzf as t8_spzf,t8.zgzf as t8_zgzf,t8.zdzf as t8_zdzf,t8.volume as t8_volume,t8.amount as t8_amount,t8.pre_close as t8_pre_close,t8.qjzf as t8_qjzf,t8.stzf as t8_stzf,t8.kxzt as t8_kxzt,t8.kp as t8_kp,t8.wp as t8_wp ,t8.sx as t8_sx,t8.xx as t8_xx,t8.ln as t8_ln,t8.zrlnb as t8_zrlnb,t8.turnover_ratio as t8_turnover_ratio,t8.change as t8_change,t8.change_pct as t8_change_pct,
         |t9.trade_date as t9_trade_date,t9.bk as t9_bk,t9.sfzt as t9_sfzt,t9.cjzt as t9_cjzt, t9.open as t9_open , t9.close as t9_close ,t9.high as t9_high,t9.low as t9_low,t9.kpzf as t9_kpzf,t9.spzf as t9_spzf,t9.zgzf as t9_zgzf,t9.zdzf as t9_zdzf,t9.volume as t9_volume,t9.amount as t9_amount,t9.pre_close as t9_pre_close,t9.qjzf as t9_qjzf,t9.stzf as t9_stzf,t9.kxzt as t9_kxzt,t9.kp as t9_kp,t9.wp as t9_wp ,t9.sx as t9_sx,t9.xx as t9_xx,t9.ln as t9_ln,t9.zrlnb as t9_zrlnb,t9.turnover_ratio as t9_turnover_ratio,t9.change as t9_change,t9.change_pct as t9_change_pct

         | from ta4 as t0
         | left join ta4 as t1 on t0.row+1=t1.row and t0.stock_code=t1.stock_code
         | left join ta4 as t2 on t0.row+2=t2.row and t0.stock_code=t2.stock_code
         | left join ta4 as t3 on t0.row+3=t3.row and t0.stock_code=t3.stock_code
         | left join ta4 as t4 on t0.row+4=t4.row and t0.stock_code=t4.stock_code
         | left join ta4 as t5 on t0.row+5=t5.row and t0.stock_code=t5.stock_code
         | left join ta4 as t6 on t0.row+6=t6.row and t0.stock_code=t6.stock_code
         | left join ta4 as t7 on t0.row+7=t7.row and t0.stock_code=t7.stock_code
         | left join ta4 as t8 on t0.row+8=t8.row and t0.stock_code=t8.stock_code
         | left join ta4 as t9 on t0.row+9=t9.row and t0.stock_code=t9.stock_code

         |""".stripMargin)

      df5.cache()
      df5.createTempView("ta5")
//      df5.show()
    spark.sql("select * from ta5")
      .repartition(8).write.mode("overwrite").partitionBy("trade_date_month").parquet("file:///D:\\gsdata\\gpsj_hs_10days_increment")

    val df6 = spark.sql(
      """|select t0.stock_code,substring(t0.trade_date,0,7) as trade_date_month,t0.trade_date as t0_trade_date,
         |t10.trade_date as t10_trade_date,t10.bk as t10_bk,t10.sfzt as t10_sfzt,t10.cjzt as t10_cjzt, t10.open as t10_open , t10.close as t10_close ,t10.high as t10_high,t10.low as t10_low,t10.kpzf as t10_kpzf,t10.spzf as t10_spzf,t10.zgzf as t10_zgzf,t10.zdzf as t10_zdzf,t10.volume as t10_volume,t10.amount as t10_amount,t10.pre_close as t10_pre_close,t10.qjzf as t10_qjzf,t10.stzf as t10_stzf,t10.kxzt as t10_kxzt,t10.kp as t10_kp,t10.wp as t10_wp ,t10.sx as t10_sx,t10.xx as t10_xx,t10.ln as t10_ln,t10.zrlnb as t10_zrlnb,t10.turnover_ratio as t10_turnover_ratio,t10.change as t10_change,t10.change_pct as t10_change_pct,
         |t11.trade_date as t11_trade_date,t11.bk as t11_bk,t11.sfzt as t11_sfzt,t11.cjzt as t11_cjzt, t11.open as t11_open , t11.close as t11_close ,t11.high as t11_high,t11.low as t11_low,t11.kpzf as t11_kpzf,t11.spzf as t11_spzf,t11.zgzf as t11_zgzf,t11.zdzf as t11_zdzf,t11.volume as t11_volume,t11.amount as t11_amount,t11.pre_close as t11_pre_close,t11.qjzf as t11_qjzf,t11.stzf as t11_stzf,t11.kxzt as t11_kxzt,t11.kp as t11_kp,t11.wp as t11_wp ,t11.sx as t11_sx,t11.xx as t11_xx,t11.ln as t11_ln,t11.zrlnb as t11_zrlnb,t11.turnover_ratio as t11_turnover_ratio,t11.change as t11_change,t11.change_pct as t11_change_pct,
         |t12.trade_date as t12_trade_date,t12.bk as t12_bk,t12.sfzt as t12_sfzt,t12.cjzt as t12_cjzt, t12.open as t12_open , t12.close as t12_close ,t12.high as t12_high,t12.low as t12_low,t12.kpzf as t12_kpzf,t12.spzf as t12_spzf,t12.zgzf as t12_zgzf,t12.zdzf as t12_zdzf,t12.volume as t12_volume,t12.amount as t12_amount,t12.pre_close as t12_pre_close,t12.qjzf as t12_qjzf,t12.stzf as t12_stzf,t12.kxzt as t12_kxzt,t12.kp as t12_kp,t12.wp as t12_wp ,t12.sx as t12_sx,t12.xx as t12_xx,t12.ln as t12_ln,t12.zrlnb as t12_zrlnb,t12.turnover_ratio as t12_turnover_ratio,t12.change as t12_change,t12.change_pct as t12_change_pct,
         |t13.trade_date as t13_trade_date,t13.bk as t13_bk,t13.sfzt as t13_sfzt,t13.cjzt as t13_cjzt, t13.open as t13_open , t13.close as t13_close ,t13.high as t13_high,t13.low as t13_low,t13.kpzf as t13_kpzf,t13.spzf as t13_spzf,t13.zgzf as t13_zgzf,t13.zdzf as t13_zdzf,t13.volume as t13_volume,t13.amount as t13_amount,t13.pre_close as t13_pre_close,t13.qjzf as t13_qjzf,t13.stzf as t13_stzf,t13.kxzt as t13_kxzt,t13.kp as t13_kp,t13.wp as t13_wp ,t13.sx as t13_sx,t13.xx as t13_xx,t13.ln as t13_ln,t13.zrlnb as t13_zrlnb,t13.turnover_ratio as t13_turnover_ratio,t13.change as t13_change,t13.change_pct as t13_change_pct,
         |t14.trade_date as t14_trade_date,t14.bk as t14_bk,t14.sfzt as t14_sfzt,t14.cjzt as t14_cjzt, t14.open as t14_open , t14.close as t14_close ,t14.high as t14_high,t14.low as t14_low,t14.kpzf as t14_kpzf,t14.spzf as t14_spzf,t14.zgzf as t14_zgzf,t14.zdzf as t14_zdzf,t14.volume as t14_volume,t14.amount as t14_amount,t14.pre_close as t14_pre_close,t14.qjzf as t14_qjzf,t14.stzf as t14_stzf,t14.kxzt as t14_kxzt,t14.kp as t14_kp,t14.wp as t14_wp ,t14.sx as t14_sx,t14.xx as t14_xx,t14.ln as t14_ln,t14.zrlnb as t14_zrlnb,t14.turnover_ratio as t14_turnover_ratio,t14.change as t14_change,t14.change_pct as t14_change_pct,
         |t15.trade_date as t15_trade_date,t15.bk as t15_bk,t15.sfzt as t15_sfzt,t15.cjzt as t15_cjzt, t15.open as t15_open , t15.close as t15_close ,t15.high as t15_high,t15.low as t15_low,t15.kpzf as t15_kpzf,t15.spzf as t15_spzf,t15.zgzf as t15_zgzf,t15.zdzf as t15_zdzf,t15.volume as t15_volume,t15.amount as t15_amount,t15.pre_close as t15_pre_close,t15.qjzf as t15_qjzf,t15.stzf as t15_stzf,t15.kxzt as t15_kxzt,t15.kp as t15_kp,t15.wp as t15_wp ,t15.sx as t15_sx,t15.xx as t15_xx,t15.ln as t15_ln,t15.zrlnb as t15_zrlnb,t15.turnover_ratio as t15_turnover_ratio,t15.change as t15_change,t15.change_pct as t15_change_pct,
         |t16.trade_date as t16_trade_date,t16.bk as t16_bk,t16.sfzt as t16_sfzt,t16.cjzt as t16_cjzt, t16.open as t16_open , t16.close as t16_close ,t16.high as t16_high,t16.low as t16_low,t16.kpzf as t16_kpzf,t16.spzf as t16_spzf,t16.zgzf as t16_zgzf,t16.zdzf as t16_zdzf,t16.volume as t16_volume,t16.amount as t16_amount,t16.pre_close as t16_pre_close,t16.qjzf as t16_qjzf,t16.stzf as t16_stzf,t16.kxzt as t16_kxzt,t16.kp as t16_kp,t16.wp as t16_wp ,t16.sx as t16_sx,t16.xx as t16_xx,t16.ln as t16_ln,t16.zrlnb as t16_zrlnb,t16.turnover_ratio as t16_turnover_ratio,t16.change as t16_change,t16.change_pct as t16_change_pct,
         |t17.trade_date as t17_trade_date,t17.bk as t17_bk,t17.sfzt as t17_sfzt,t17.cjzt as t17_cjzt, t17.open as t17_open , t17.close as t17_close ,t17.high as t17_high,t17.low as t17_low,t17.kpzf as t17_kpzf,t17.spzf as t17_spzf,t17.zgzf as t17_zgzf,t17.zdzf as t17_zdzf,t17.volume as t17_volume,t17.amount as t17_amount,t17.pre_close as t17_pre_close,t17.qjzf as t17_qjzf,t17.stzf as t17_stzf,t17.kxzt as t17_kxzt,t17.kp as t17_kp,t17.wp as t17_wp ,t17.sx as t17_sx,t17.xx as t17_xx,t17.ln as t17_ln,t17.zrlnb as t17_zrlnb,t17.turnover_ratio as t17_turnover_ratio,t17.change as t17_change,t17.change_pct as t17_change_pct,
         |t18.trade_date as t18_trade_date,t18.bk as t18_bk,t18.sfzt as t18_sfzt,t18.cjzt as t18_cjzt, t18.open as t18_open , t18.close as t18_close ,t18.high as t18_high,t18.low as t18_low,t18.kpzf as t18_kpzf,t18.spzf as t18_spzf,t18.zgzf as t18_zgzf,t18.zdzf as t18_zdzf,t18.volume as t18_volume,t18.amount as t18_amount,t18.pre_close as t18_pre_close,t18.qjzf as t18_qjzf,t18.stzf as t18_stzf,t18.kxzt as t18_kxzt,t18.kp as t18_kp,t18.wp as t18_wp ,t18.sx as t18_sx,t18.xx as t18_xx,t18.ln as t18_ln,t18.zrlnb as t18_zrlnb,t18.turnover_ratio as t18_turnover_ratio,t18.change as t18_change,t18.change_pct as t18_change_pct,
         |t19.trade_date as t19_trade_date,t19.bk as t19_bk,t19.sfzt as t19_sfzt,t19.cjzt as t19_cjzt, t19.open as t19_open , t19.close as t19_close ,t19.high as t19_high,t19.low as t19_low,t19.kpzf as t19_kpzf,t19.spzf as t19_spzf,t19.zgzf as t19_zgzf,t19.zdzf as t19_zdzf,t19.volume as t19_volume,t19.amount as t19_amount,t19.pre_close as t19_pre_close,t19.qjzf as t19_qjzf,t19.stzf as t19_stzf,t19.kxzt as t19_kxzt,t19.kp as t19_kp,t19.wp as t19_wp ,t19.sx as t19_sx,t19.xx as t19_xx,t19.ln as t19_ln,t19.zrlnb as t19_zrlnb,t19.turnover_ratio as t19_turnover_ratio,t19.change as t19_change,t19.change_pct as t19_change_pct

         | from ta4 as t0
         | left join ta4 as t10 on t0.row+10=t10.row and t0.stock_code=t10.stock_code
         | left join ta4 as t11 on t0.row+11=t11.row and t0.stock_code=t11.stock_code
         | left join ta4 as t12 on t0.row+12=t12.row and t0.stock_code=t12.stock_code
         | left join ta4 as t13 on t0.row+13=t13.row and t0.stock_code=t13.stock_code
         | left join ta4 as t14 on t0.row+14=t14.row and t0.stock_code=t14.stock_code
         | left join ta4 as t15 on t0.row+15=t15.row and t0.stock_code=t15.stock_code
         | left join ta4 as t16 on t0.row+16=t16.row and t0.stock_code=t16.stock_code
         | left join ta4 as t17 on t0.row+17=t17.row and t0.stock_code=t17.stock_code
         | left join ta4 as t18 on t0.row+18=t18.row and t0.stock_code=t18.stock_code
         | left join ta4 as t19 on t0.row+19=t19.row and t0.stock_code=t19.stock_code

         |""".stripMargin)


    df6.cache()
    df6.createTempView("ta6")
//    df6.show()

    spark.sql("select * from ta6")
      .repartition(8).write.mode("overwrite").partitionBy("trade_date_month").parquet("file:///D:\\gsdata\\gpsj_hs_h10days_increment")

    all20days(spark)

    //TODO 更新三个数据集除第一个月的数据的其他数据
    FileTools.dataFrame2Increment(spark:SparkSession,"gpsj_hs_10days","gpsj_hs_10days_increment",m2,m3)
    FileTools.dataFrame2Increment(spark:SparkSession,"gpsj_hs_h10days","gpsj_hs_h10days_increment",m2,m3)
    FileTools.dataFrame2Increment(spark:SparkSession,"gpsj_hs_20days","gpsj_hs_20days_increment",m2,m3)

    df2.unpersist()
    df3.unpersist()
    df4.unpersist()
    df5.unpersist()
    df6.unpersist()

    val endm = System.currentTimeMillis()
    println("共耗时："+(endm-startm)/1000+"秒")
    spark.close()
  }

  /**
   * 20天为一个月基本开盘时间量
   * @param spark
   */
  def all20days(spark:SparkSession)={
    val df1 = spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_10days_increment").repartition(1000)
    df1.createTempView("wt1")

    val df2 = spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_h10days_increment").repartition(1000)
    df2.createTempView("wt2")

    val duplicateColumns = df1.columns.intersect(df2.columns)
    val wt2str = df2.columns.diff(duplicateColumns).map(f=>"wt2."+f).mkString(",")

    val df3 = spark.sql(s"select wt1.*,$wt2str  from wt1 left join wt2 on wt1.stock_code=wt2.stock_code and wt1.t0_trade_date=wt2.t0_trade_date")
//      df3.show()
      df3.repartition(6)
      .write.mode("overwrite").partitionBy("trade_date_month").parquet("file:///D:\\gsdata\\gpsj_hs_20days_increment")

  }


  /**
   * 【先删后加】增量更新，将更新的分区数据移动（改名），然后更新新的数据按照分区增量更
   * 注意：犹豫宽表
   * @param spark
   * @param df
   * @param CompleteSinkPath
   * @param IncrementSinkPath
   */
  def updateGpsjIncrement(spark:SparkSession,df:DataFrame,CompleteSinkPath:String,IncrementSinkPath:String)={
    val date = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())

    df.distinct().repartition(1).write.mode("overwrite")
      .partitionBy("trade_date_month")
      .parquet(s"file:///D:\\gsdata\\$IncrementSinkPath")

    //读取数据，确定分区
    val df2 = spark.read.parquet(s"file:///D:\\gsdata\\$IncrementSinkPath")
    val pathArray = df2.withColumn("file_name", input_file_name())
      .select("file_name")
      .distinct().collect()
    pathArray.foreach(f=>{
      //source源目录，sink目标目录，archieve归档目录
      val path = f.getAs[String]("file_name").replaceAll("file:///","")
      val source = path.split("/").dropRight(1).mkString("/")
      val sink = path.split("/").dropRight(1).mkString("/").replaceAll(IncrementSinkPath,CompleteSinkPath)
      val achieve = path.split("/").dropRight(1).mkString("/").replaceAll(IncrementSinkPath,CompleteSinkPath).replaceAll(CompleteSinkPath,"achieve/"+CompleteSinkPath+"_"+date)
      val achieveTarget = achieve.split("/").dropRight(1).mkString("/")
      //判断路径是否存在，如果存在则迁移到归档去
      println(source)
      println(sink)
      println(achieve)
      println(achieveTarget)

      FileTools.incrementRename(source,sink,achieve)

    })
  }
}
