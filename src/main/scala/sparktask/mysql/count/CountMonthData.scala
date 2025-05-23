package sparktask.mysql.count

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CountMonthData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.local.dir","D:\\Temp")
    val spark = SparkSession
      .builder()
      .appName("CountMonthData")
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

//    val df1 = spark.read.parquet("file:///D:\\gsdata\\gpsj_9days\\trade_date_month=2024-03")
//    df1.printSchema()
//    df1.show()

//    val df2 = spark.read.parquet("file:///D:\\gsdata\\gpsj_h6days\\trade_date_month=2024-03")
//    df2.printSchema()
//    df2.show()

//    val df2 = spark.read.parquet("file:///D:\\gsdata\\gpsj_9days_month\\trade_date_month=2024-03")
//    val d = df1.except(df2)
//    print(d.count)

     val df15 = spark.read.parquet("file:///D:\\gsdata\\gpsj_15days\\trade_date_month=2024-03")

     df15.createTempView("ta2")

//    spark.sql("select * from ta2 order by t0_trade_date desc").show()
    //   and t6_change_pct<0 and t6_ln='缩量' and t6_zt like '绿柱%'
    //   and t7_change_pct<0 and t7_ln='缩量' and t7_zt like '绿柱%'
    //   and t8_change_pct<0 and t8_ln='缩量' and t8_zt like '绿柱%'
    // if(abs(t5_change_pct)<2 and t5_zt like '%红柱下影线','t5',if(abs(t5_change_pct)<2 and t6_zt like '%红柱下影线','t6',if(abs(t5_change_pct)<2 and t7_zt like '%红柱下影线','t7',if(abs(t5_change_pct)<2 and t8_zt like '%红柱下影线','t8',if(abs(t5_change_pct)<2 and t9_zt like '%红柱下影线','t9',if(abs(t5_change_pct)<2 and t10_zt like '%红柱下影线','t10',if(abs(t5_change_pct)<2 and t11_zt like '%红柱下影线','t11',if(abs(t5_change_pct)<2 and t12_zt like '%红柱下影线','t12','')))))))) as trq,
    //if(abs(t5_change_pct)<2 and t5_zt like '%红柱下影线',t6_change_pct,if(abs(t5_change_pct)<2 and t6_zt like '%红柱下影线',t7_change_pct,if(abs(t5_change_pct)<2 and t7_zt like '%红柱下影线',t8_change_pct,if(abs(t5_change_pct)<2 and t8_zt like '%红柱下影线',t9_change_pct,if(abs(t5_change_pct)<2 and t9_zt like '%红柱下影线',t10_change_pct,if(abs(t5_change_pct)<2 and t10_zt like '%红柱下影线',t11_change_pct,if(abs(t5_change_pct)<2 and t11_zt like '%红柱下影线',t12_change_pct,if(abs(t5_change_pct)<2 and t12_zt like '%红柱下影线',t13_change_pct,'')))))))) as tp
    /**
     * 购买形式：
     * 1、深跌找微笑线
     * 2、第三天跌幅小于百分之2
     *
     * 不能买形式：
     * 1、一字板后不能买
     */
//  and t8_kp='高开'
//    val df = spark.sql(
//      """select
//        | t4_trade_date,t7_change_pct,t8_change_pct,t9_change_pct,t10_change_pct,t11_change_pct,*
//        | from ta2
//        | where abs(t2_change_pct)<5 and abs(t3_change_pct)<5
//        |   and t4_change_pct>8 and t4_ln in('放量','放巨量') and t4_zt not in ('正T','一字')
//            and t5_high<t4_high and t5_change_pct<0 and t5_zt like '绿柱%'
//        |   and t6_change_pct<0 and t6_ln ='缩量' and t6_zt like '绿柱%'
//        |   and t7_change_pct<0 and t7_ln ='缩量' and t7_zt like '绿柱%'
//        |
//
//        | order by t0_trade_date
//        |""".stripMargin)
//      df.show(300)
//    df.createTempView("ta3")
//
//
//    spark.sql("select sum(t8_change_pct) as p1,sum(t8_change_pct+t9_change_pct) as p2,sum(t8_change_pct+t9_change_pct+t10_change_pct) as p3 from ta3").show()


    //and (t6_change_pct>9 or t7_change_pct>9 or t8_change_pct>9 or t9_change_pct>9 or t10_change_pct>9 or t11_change_pct>9 or t12_change_pct>9)
//    val df = spark.sql(
//      """select stock_code,t2_trade_date,
//        |t2_change_pct,t3_change_pct,t4_change_pct,t5_change_pct,t6_change_pct,t7_change_pct,t8_change_pct,
//        |t9_change_pct ,t10_change_pct,t11_change_pct,t12_change_pct,t13_change_pct
//        |from ta2 where abs(t0_change_pct)<2 and abs(t1_change_pct)<2
//        |   and t2_change_pct>9 and t2_ln in('放量','放巨量') and t2_zt not in ('正T','一字')
//        |   and t3_change_pct<0
//        |   and t4_change_pct<0 and t4_ln ='缩量'
//        |   and t5_change_pct<0 and t5_ln ='缩量'
//        |   and t6_kp='高开'
//        | order by t0_trade_date""".stripMargin)
//      df.show(300)
//    df.createTempView("tt1")
//    spark.sql("select sum(tp) from tt1").show()

//val df = spark.sql(
//        """select
//          | t4_trade_date,t1_change_pct,t2_change_pct,t3_change_pct,t4_change_pct,t5_change_pct,t6_change_pct,t7_change_pct,t8_change_pct,t9_change_pct,t10_change_pct,t11_change_pct,*
//          | from ta2
//          | where abs(t0_change_pct)<3 and abs(t1_change_pct)<3 and abs(t2_change_pct)<3 and abs(t3_change_pct)<3 and abs(t4_change_pct)<3
//          |   and t5_change_pct>9 and t5_ln in('放量','放巨量') and t5_zt not in ('正T','一字')
//          |   and t6_change_pct<0 and t7_change_pct<0 and t8_change_pct<0
//          | order by t0_trade_date
//          |""".stripMargin)
//        df.show(300)
//      df.createTempView("ta3")
//
//
//      spark.sql("select sum(t9_change_pct) as p1,sum(t9_change_pct+t10_change_pct) as p2,sum(t9_change_pct+t10_change_pct+t11_change_pct) as p3 from ta3").show()

    //or t8_zt='红柱下影线' or t9_zt='红柱下影线' or t10_zt='红柱下影线' or t11_zt='红柱下影线'
    val df = spark.sql(
      """select stock_code,t3_change_pct,t4_change_pct,t5_change_pct,t6_change_pct,t7_trade_date,t7_zt,t8_change_pct,t8_zt,t9_change_pct,t9_zt,t10_change_pct,t10_zt,t11_change_pct,t11_zt,t12_change_pct from ta2
        |where abs(t0_change_pct)<2 and abs(t1_change_pct)<2
        |and t2_change_pct>9 and t3_change_pct>0 and t3_change_pct<3
        |
        |
       """.stripMargin)
  df.show(200)
    df.createTempView("ta3")
    spark.sql("select sum(t4_change_pct) as p1,sum(t4_change_pct+t5_change_pct) as p2,sum(t4_change_pct+t5_change_pct+t6_change_pct) as p3 from ta3").show()

    spark.close()
  }
}
