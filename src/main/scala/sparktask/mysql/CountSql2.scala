package sparktask.mysql

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, IntegerType}

import scala.collection.mutable.ArrayBuffer

/**
 * 获取9日的宽表作分析
 */
object CountSql2 {
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

    val url = "jdbc:mysql://localhost:3306/gs"

    val driver = "com.mysql.cj.jdbc.Driver"
    val user = "root"
    val pwd = "123456"

    val properties = new Properties()
    properties.setProperty("user",user)
    properties.setProperty("password",pwd)
    properties.setProperty("url",url)
    properties.setProperty("driver",driver)
    val jyrl = "data2024_gpsj_day_0318"
    import spark.implicits._
    spark.read.jdbc(url, jyrl, properties).createTempView("ta1")

    //按月保存每日数据
    //    spark.sql("select *,substring(trade_date,0,7) as trade_date_month from ta1 where trade_time='2024-03-18 00:00:00' order by stock_code").drop("index")
    //      .distinct().repartition(1).write.mode("append").partitionBy("trade_date_month").parquet("file:///D:\\gsdata\\gpsj_day")

//    spark.read.parquet("file:///D:\\gsdata\\gpsj_day\\trade_date_month=2023-*")
    spark.read.parquet("file:///D:\\gsdata\\gpsj_day")
      .withColumn("open",col("open").cast(DecimalType(10, 2)))
      .withColumn("close",col("close").cast(DecimalType(10, 2)))
      .withColumn("volume",col("volume").cast(IntegerType))
      .withColumn("high",col("high").cast(DecimalType(10, 2)))
      .withColumn("low",col("low").cast(DecimalType(10, 2)))
      .withColumn("amount",col("amount").cast(IntegerType))
      .withColumn("change",col("change").cast(DecimalType(10, 2)))
      .withColumn("change_pct",col("change_pct").cast(DecimalType(10, 2)))
      .withColumn("pre_close",col("pre_close").cast(DecimalType(10, 2)))
      .createTempView("ta2")
    //    spark.sql("select * from ta2").show(false)
    spark.sql("select *,row_number() over(partition by stock_code order by trade_date,stock_code) as row from ta2 ").createTempView("ta3")

    spark.sql(
      """select t2.*,
        |if(t2.open<t1.close,'低开',if(t2.open=t1.close,'平开','高开')) as kp,
        |if(t2.open<t2.close,'高走',if(t2.open=t1.close,'平走','低走')) as wp,
        |if(t2.high<t1.high,'下方',if(t2.high=t1.high,'平方','上方')) as sx,
        |if(t2.low<t1.low,'下方',if(t2.low=t1.low,'平方','上方')) as xx,
        |if(t2.volume<t1.volume,'缩量',if(t2.volume=t1.volume,'平量',if(t2.volume/t1.volume>2,'放巨量','放量'))) as ln,
        |if(t2.open>t2.close,if(greatest(t2.open-t2.close,t2.high-t2.open,t2.close-t2.low)=t2.open-t2.close,'绿柱',if(greatest(t2.open-t2.close,t2.high-t2.open,t2.close-t2.low)=t2.high-t2.open,'绿柱上影线','绿柱下影线')),if(t2.open=t2.close,if(greatest(t2.open-t2.close,t2.high-t2.open,t2.close-t2.low)=t2.open-t2.close,'一字',if(greatest(t2.open-t2.close,t2.high-t2.open,t2.close-t2.low)=t2.high-t2.open,'倒T','正T')),if(greatest(t2.close-t2.open,t2.high-t2.close,t2.open-t2.low)=t2.close-t2.open,'红柱',if(greatest(t2.close-t2.open,t2.high-t2.close,t2.open-t2.low)=t2.high-t2.close,'红柱上影线','红柱下影线')))) as zt
        |from ta3 as t1 left join ta3 as t2 on t1.row+1=t2.row and t1.stock_code=t2.stock_code
        |where t2.row is not null and t2.stock_code like '00%' or t2.stock_code like '60%'""".stripMargin).createTempView("ta4")

    spark.sql("select * from ta4").printSchema()
//    spark.sql("select * from ta4 where zt in ('绿柱下影线','红柱下影线','正T') and trade_date='2024-03-15'").show(100,false)

//    spark.sql(
//      """select t1.stock_code,t1.trade_date,t1.ln,t1.change_pct,t2.change_pct,t3.change_pct,t4.change_pct,
//        |t5.open,t5.high,t5.low,t5.close,t5.change_pct,t5.zt,t6.open,t6.high,t6.low,t6.close,t6.change_pct,t6.zt,
//        |t6.open-t5.open as jjc,
//        |(t6.open-t5.open)/t5.open as jjzdf ,
//        |((t6.high+t6.open)/2-(t5.open+t5.low)/2)/((t5.open+t5.low)/2) as jjzdf2,
//        |((t6.high+t6.low)/2-(t5.high+t5.low)/2)/((t5.high+t5.low)/2) as jjzdf3
//        |from ta4 as t1
//        |left join ta4 as t2 on t1.row+1=t2.row and t1.stock_code=t2.stock_code
//        |left join ta4 as t3 on t1.row+2=t3.row and t1.stock_code=t3.stock_code
//        |left join ta4 as t4 on t1.row+3=t4.row and t1.stock_code=t4.stock_code
//        |left join ta4 as t5 on t1.row+4=t5.row and t1.stock_code=t5.stock_code
//        |left join ta4 as t6 on t1.row+5=t6.row and t1.stock_code=t6.stock_code
//        |where t1.change_pct>6 and (t1.ln='放量' or t1.ln='放巨量')
//        | and t2.kp='低开' and t2.wp='低走' and t2.sx='下方' and t2.ln='缩量'
//        | and t3.kp='低开' and t3.wp='低走' and t3.sx='下方' and t3.ln='缩量'
//        | and t4.kp='低开' and t4.wp='低走' and t4.sx='下方' and t4.ln='缩量'
//        | and t2.change_pct<t3.change_pct and t3.change_pct<t4.change_pct
//        | and abs(t2.change_pct+t3.change_pct+t4.change_pct)/t1.change_pct>0.75
//        | and t5.zt='红柱下影线'
//        | order by trade_date
//        |""".stripMargin).createTempView("ta5")

    //TODO 微笑线
    /**
     *
     *  and t5.zt='红柱下影线'
     */
    spark.sql(
      """|select t0.stock_code,substring(t0.trade_date,0,7) as trade_date_month,
         |t0.trade_date as t0_trade_date, t0.open as t0_open , t0.close as t0_close ,t0.high as t0_high,t0.low as t0_low,t0.kp as t0_kp,t0.wp as t0_wp ,t0.sx as t0_sx,t0.xx as t0_xx,t0.ln as t0_ln,t0.zt as t0_zt,t0.change_pct as t0_change_pct,
         |t1.trade_date as t1_trade_date, t1.open as t1_open , t1.close as t1_close ,t1.high as t1_high,t1.low as t1_low,t1.kp as t1_kp,t1.wp as t1_wp ,t1.sx as t1_sx,t1.xx as t1_xx,t1.ln as t1_ln,t1.zt as t1_zt,t1.change_pct as t1_change_pct,
         |t2.trade_date as t2_trade_date, t2.open as t2_open , t2.close as t2_close ,t2.high as t2_high,t2.low as t2_low,t2.kp as t2_kp,t2.wp as t2_wp ,t2.sx as t2_sx,t2.xx as t2_xx,t2.ln as t2_ln,t2.zt as t2_zt,t2.change_pct as t2_change_pct,
         |t3.trade_date as t3_trade_date, t3.open as t3_open , t3.close as t3_close ,t3.high as t3_high,t3.low as t3_low,t3.kp as t3_kp,t3.wp as t3_wp ,t3.sx as t3_sx,t3.xx as t3_xx,t3.ln as t3_ln,t3.zt as t3_zt,t3.change_pct as t3_change_pct,
         |t4.trade_date as t4_trade_date, t4.open as t4_open , t4.close as t4_close ,t4.high as t4_high,t4.low as t4_low,t4.kp as t4_kp,t4.wp as t4_wp ,t4.sx as t4_sx,t4.xx as t4_xx,t4.ln as t4_ln,t4.zt as t4_zt,t4.change_pct as t4_change_pct,
         |t5.trade_date as t5_trade_date, t5.open as t5_open , t5.close as t5_close ,t5.high as t5_high,t5.low as t5_low,t5.kp as t5_kp,t5.wp as t5_wp ,t5.sx as t5_sx,t5.xx as t5_xx,t5.ln as t5_ln,t5.zt as t5_zt,t5.change_pct as t5_change_pct,
         |t6.trade_date as t6_trade_date, t6.open as t6_open , t6.close as t6_close ,t6.high as t6_high,t6.low as t6_low,t6.kp as t6_kp,t6.wp as t6_wp ,t6.sx as t6_sx,t6.xx as t6_xx,t6.ln as t6_ln,t6.zt as t6_zt,t6.change_pct as t6_change_pct,
         |t7.trade_date as t7_trade_date, t7.open as t7_open , t7.close as t7_close ,t7.high as t7_high,t7.low as t7_low,t7.kp as t7_kp,t7.wp as t7_wp ,t7.sx as t7_sx,t7.xx as t7_xx,t7.ln as t7_ln,t7.zt as t7_zt,t7.change_pct as t7_change_pct,
         |t8.trade_date as t8_trade_date, t8.open as t8_open , t8.close as t8_close ,t8.high as t8_high,t8.low as t8_low,t8.kp as t8_kp,t8.wp as t8_wp ,t8.sx as t8_sx,t8.xx as t8_xx,t8.ln as t8_ln,t8.zt as t8_zt,t8.change_pct as t8_change_pct

         | from ta4 as t0
         | left join ta4 as t1 on t0.row+1=t1.row and t0.stock_code=t1.stock_code
         | left join ta4 as t2 on t0.row+2=t2.row and t0.stock_code=t2.stock_code
         | left join ta4 as t3 on t0.row+3=t3.row and t0.stock_code=t3.stock_code
         | left join ta4 as t4 on t0.row+4=t4.row and t0.stock_code=t4.stock_code
         | left join ta4 as t5 on t0.row+5=t5.row and t0.stock_code=t5.stock_code
         | left join ta4 as t6 on t0.row+6=t6.row and t0.stock_code=t6.stock_code
         | left join ta4 as t7 on t0.row+7=t7.row and t0.stock_code=t7.stock_code
         | left join ta4 as t8 on t0.row+8=t8.row and t0.stock_code=t8.stock_code

         |""".stripMargin).createTempView("ta5")

    spark.sql("select * from ta5")
      .repartition(5).write.mode("overwrite").partitionBy("trade_date_month").parquet("file:///D:\\gsdata\\gpsj_9days")

    spark.sql(
      """|select t0.stock_code,substring(t0.trade_date,0,7) as trade_date_month,
         |t0.trade_date as t0_trade_date,
         |t9.trade_date as t9_trade_date, t9.open as t9_open , t9.close as t9_close ,t9.high as t9_high,t9.low as t9_low,t9.kp as t9_kp,t9.wp as t9_wp ,t9.sx as t9_sx,t9.xx as t9_xx,t9.ln as t9_ln,t9.zt as t9_zt,t9.change_pct as t9_change_pct,
         |t10.trade_date as t10_trade_date, t10.open as t10_open , t10.close as t10_close ,t10.high as t10_high,t10.low as t10_low,t10.kp as t10_kp,t10.wp as t10_wp ,t10.sx as t10_sx,t10.xx as t10_xx,t10.ln as t10_ln,t10.zt as t10_zt,t10.change_pct as t10_change_pct,
         |t11.trade_date as t11_trade_date, t11.open as t11_open , t11.close as t11_close ,t11.high as t11_high,t11.low as t11_low,t11.kp as t11_kp,t11.wp as t11_wp ,t11.sx as t11_sx,t11.xx as t11_xx,t11.ln as t11_ln,t11.zt as t11_zt,t11.change_pct as t11_change_pct,
         |t12.trade_date as t12_trade_date, t12.open as t12_open , t12.close as t12_close ,t12.high as t12_high,t12.low as t12_low,t12.kp as t12_kp,t12.wp as t12_wp ,t12.sx as t12_sx,t12.xx as t12_xx,t12.ln as t12_ln,t12.zt as t12_zt,t12.change_pct as t12_change_pct,
         |t13.trade_date as t13_trade_date, t13.open as t13_open , t13.close as t13_close ,t13.high as t13_high,t13.low as t13_low,t13.kp as t13_kp,t13.wp as t13_wp ,t13.sx as t13_sx,t13.xx as t13_xx,t13.ln as t13_ln,t13.zt as t13_zt,t13.change_pct as t13_change_pct,
         |t14.trade_date as t14_trade_date, t14.open as t14_open , t14.close as t14_close ,t14.high as t14_high,t14.low as t14_low,t14.kp as t14_kp,t14.wp as t14_wp ,t14.sx as t14_sx,t14.xx as t14_xx,t14.ln as t14_ln,t14.zt as t14_zt,t14.change_pct as t14_change_pct,
         |t15.trade_date as t15_trade_date, t15.open as t15_open , t15.close as t15_close ,t15.high as t15_high,t15.low as t15_low,t15.kp as t15_kp,t15.wp as t15_wp ,t15.sx as t15_sx,t15.xx as t15_xx,t15.ln as t15_ln,t15.zt as t15_zt,t15.change_pct as t15_change_pct,
         |t16.trade_date as t16_trade_date, t16.open as t16_open , t16.close as t16_close ,t16.high as t16_high,t16.low as t16_low,t16.kp as t16_kp,t16.wp as t16_wp ,t16.sx as t16_sx,t16.xx as t16_xx,t16.ln as t16_ln,t16.zt as t16_zt,t16.change_pct as t16_change_pct
         | from ta4 as t0
         | left join ta4 as t9 on t0.row+9=t9.row and t0.stock_code=t9.stock_code
         | left join ta4 as t10 on t0.row+10=t10.row and t0.stock_code=t10.stock_code
         | left join ta4 as t11 on t0.row+11=t11.row and t0.stock_code=t11.stock_code
         | left join ta4 as t12 on t0.row+12=t12.row and t0.stock_code=t12.stock_code
         | left join ta4 as t13 on t0.row+13=t13.row and t0.stock_code=t13.stock_code
         | left join ta4 as t14 on t0.row+14=t14.row and t0.stock_code=t14.stock_code
         | left join ta4 as t15 on t0.row+15=t15.row and t0.stock_code=t15.stock_code
         | left join ta4 as t16 on t0.row+16=t16.row and t0.stock_code=t16.stock_code
         |""".stripMargin).createTempView("ta6")

    spark.sql("select * from ta6")
      .repartition(5).write.mode("overwrite").partitionBy("trade_date_month").parquet("file:///D:\\gsdata\\gpsj_h9days")

//    spark.sql("""select sum(jjzdf),sum(jjzdf2),sum(jjzdf3) from ta5""")
//      .show(300,false)

    spark.close()
  }
}
