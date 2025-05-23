package sparktask.mysql

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, IntegerType}

import scala.collection.mutable.ArrayBuffer

object CountSql {
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
    spark.sql("select *,substring(trade_date,0,7) as trade_date_month from ta1 where trade_time='2024-03-18 00:00:00' order by stock_code").drop("index")
      .distinct().repartition(1).write.mode("append").partitionBy("trade_date_month").parquet("file:///D:\\gsdata\\gpsj_day")

//    spark.read.parquet("file:///D:\\gsdata\\gpsj_day\\trade_date_month=2024-0*")
//      .withColumn("open",col("open").cast(DecimalType(10, 2)))
//      .withColumn("close",col("close").cast(DecimalType(10, 2)))
//      .withColumn("volume",col("volume").cast(IntegerType))
//      .withColumn("high",col("high").cast(DecimalType(10, 2)))
//      .withColumn("low",col("low").cast(DecimalType(10, 2)))
//      .withColumn("amount",col("amount").cast(IntegerType))
//      .withColumn("change",col("change").cast(DecimalType(10, 2)))
//      .withColumn("change_pct",col("change_pct").cast(DecimalType(10, 2)))
//      .withColumn("pre_close",col("pre_close").cast(DecimalType(10, 2)))
//      .createTempView("ta2")
////    spark.sql("select * from ta2").show(false)
//    spark.sql("select *,row_number() over(partition by stock_code order by trade_date,stock_code) as row from ta2 ").createTempView("ta3")
//
//    /**
//     * 只做竞价
//     * kp：开盘  低开、高开、平开
//     * wp：尾盘  低走、平走、高走
//     * sx：上线  下方、上方、平方
//     * xx：下线  下方、上方、平方
//     * ln：量能  放量、缩量
//     * yx：引线  上引线、下引线、红柱、绿柱、十字星
//     */
//
//    spark.sql(
//      """select t2.*,
//        |if(t2.open<t1.close,'低开',if(t2.open=t1.close,'平开','高开')) as kp,
//        |if(t2.open<t2.close,'高走',if(t2.open=t1.close,'平走','低走')) as wp,
//        |if(t2.high<t1.high,'下方',if(t2.high=t1.high,'平方','上方')) as sx,
//        |if(t2.low<t1.low,'下方',if(t2.low=t1.low,'平方','上方')) as xx,
//        |if(t2.volume<t1.volume,'缩量',if(t2.volume=t1.volume,'平量',if(t2.volume/t1.volume>2,'放巨量','放量'))) as ln,
//        |if(t2.open>t2.close,if((t2.high-t2.open)/(t2.close-t2.low)>1,'绿柱上影线','绿柱下影线'),if(t2.open=t2.close,'十字星',if((t2.high-t2.close)/(t2.open-t2.low)>1,'红柱上影线','红柱下影线'))) as yx
//        |from ta3 as t1 left join ta3 as t2 on t1.row+1=t2.row and t1.stock_code=t2.stock_code
//        |where t2.row is not null and t2.stock_code like '00%' or t2.stock_code like '60%'""".stripMargin).createTempView("ta4")
//
//    /**
//     * 两天缩量
//     */
////    spark.sql(
////      """select t1.stock_code,t1.trade_date,t1.change_pct,t2.change_pct,t3.change_pct,t4.change_pct,t5.open,t5.high,t5.low,t5.close,t5.change_pct,t6.open,t6.high,t6.low,t6.close,t6.change_pct from ta4 as t1
////        |left join ta4 as t2 on t1.row+1=t2.row and t1.stock_code=t2.stock_code
////        |left join ta4 as t3 on t1.row+2=t3.row and t1.stock_code=t3.stock_code
////        |left join ta4 as t4 on t1.row+3=t4.row and t1.stock_code=t4.stock_code
////        |left join ta4 as t5 on t1.row+4=t5.row and t1.stock_code=t5.stock_code
////        |left join ta4 as t6 on t1.row+5=t6.row and t1.stock_code=t6.stock_code
////        |where t1.change_pct>8 and t1.ln='放量'
////        | and t2.kp='低开' and t2.wp='低走' and t2.sx='下方' and t2.ln='缩量'
////        | and t3.kp='低开' and t3.wp='低走' and t3.sx='下方' and t3.ln='缩量'
////        | and abs(t2.change_pct+t3.change_pct+t4.change_pct)/t1.change_pct>0.75
////        |""".stripMargin).show()
//
//    /**
//     * 三天缩量
//     */
//        spark.sql(
//      """select t1.stock_code,t1.trade_date,t1.ln,t1.change_pct,t2.change_pct,t3.change_pct,t4.change_pct,
//        |t5.open,t5.high,t5.low,t5.close,t5.change_pct,t6.open,t6.high,t6.low,t6.close,t6.change_pct,
//        |t5.yx,t6.yx,
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
//        | order by trade_date
//        |""".stripMargin).createTempView("ta5")
//
//          spark.sql("select * from ta5").show(300,false)
//
//          spark.sql("""select sum(jjzdf),sum(jjzdf2),sum(jjzdf3) from ta5""")
//          .show(300,false)




    spark.close()
  }
}
