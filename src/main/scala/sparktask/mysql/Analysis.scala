package sparktask.mysql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Analysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled","true")
    val spark = SparkSession
      .builder()
      .appName("SqlIn")
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark.read.parquet("file:///D:\\gsdata\\gpsj_9days\\trade_date_month=2020-*").createTempView("ta2")

    // and t0_trade_date >'2024-03-07'
    val df = spark.sql(
      """select
        | stock_code,t0_trade_date,t6_open,t6_close,t6_high,t6_low,t6_kp,t6_wp,t6_sx,t6_xx, t6_ln, t6_zt,t6_change_pct,t7_open,t7_close,t7_high,t7_low,t7_kp,t7_wp,t7_sx,t7_xx, t7_ln,t7_zt,t7_change_pct,t8_open,t8_close,t8_high,t8_low,t8_kp,t8_wp,t8_sx,t8_xx, t8_ln,t8_zt,t8_change_pct
        | from ta2
        |where  t2_change_pct>8 and t2_ln in('放量','放巨量')
        | and t3_change_pct<0 and t3_zt like '绿柱%'
        | and t4_change_pct<0 and t4_ln='缩量' and t4_zt like '绿柱%'
        | and t5_change_pct<0 and t5_ln='缩量' and t5_zt like '绿柱%'
        |order by t0_trade_date
        |""".stripMargin)
      df.createTempView("ta3")
      df.show(300)
      spark.sql("select sum(t6_change_pct) as p1,sum(t6_change_pct+t7_change_pct) as p2 from ta3").show()


//     val df = spark.sql(
//        """
//          |select stock_code,t0_trade_date, t0_change_pct,t1_change_pct,t2_change_pct,t3_change_pct,t4_change_pct,t5_change_pct,t6_change_pct,t7_change_pct,t8_change_pct,
//          | t6_open,t6_close,t6_high,t6_low,t6_kp,t6_wp,
//          | t7_open,t7_close,t7_high,t7_low,t7_kp,t7_wp,
//          | t8_open,t8_close,t8_high,t8_low,t8_kp,t8_wp,
//          | (((t7_open+t7_high)/2)-((t6_open+t6_low)/2))/((t6_open+t6_low)/2) as p1,
//          | (((t8_open+t8_high)/2)-((t6_open+t6_low)/2))/((t6_open+t6_low)/2) as p2,
//          | t6_change_pct+t7_change_pct as 2p,t6_change_pct+t7_change_pct+t8_change_pct as 3p
//          | from ta2
//          |where abs(t0_change_pct)<3 and abs(t1_change_pct)<3
//          | and t2_change_pct>8 and t2_ln in('放量','放巨量')
//          | and t3_change_pct<0 and t3_zt like '绿柱%'
//          | and t4_change_pct<0 and t4_ln='缩量' and t4_zt like '绿柱%'
//          | and t5_change_pct<0 and t5_change_pct>-3 and t5_ln='缩量' and t5_zt like '绿柱%'
//          | and t6_kp='低开'
//          | order by t0_trade_date
//          |""".stripMargin)
//      df.createTempView("ta3")
//        spark.sql("select * from ta3").show(300)
//
//    spark.sql("select 1p*d as 1p,2p*d as 2p,3p*d as 3p,p1*d as p1,p2*d as p2 from  (select sum(t6_change_pct) as 1p,sum(2p) as 2p,sum(3p) as 3p,sum(p1) as p1,sum(p2) as p2,count(distinct t0_trade_date)/count(1) as d from ta3)")
//      .show(300)



    spark.close()
  }
}
