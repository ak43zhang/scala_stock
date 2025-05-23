package sparktask.mysql.update.demotest

import java.text.DecimalFormat

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import sparktask.mysql.update.demotest.Demo1.zf1_zp

/**
 * 用于测试
 */
object Demo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.hadoop.skip.checksum","true")
      .set("spark.local.dir","D:\\Temp")
    val spark = SparkSession
      .builder()
      .appName("Demo1")
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    for(i<-1994 to 2024){
      for(j<-1 to 1){
        val format = new DecimalFormat("00")
        val formatted = format.format(j)
//        val df1 = spark.read.parquet(s"file:///D:\\gsdata\\gpsj_15days\\trade_date_month=$i-$formatted")
        val df1 = spark.read.parquet(s"file:///D:\\gsdata\\gpsj_15days\\trade_date_month=$i*")
        println("===============================")
        println(s"==========$i-$j===============")
        zf(spark,df1)
        println("===============================")
      }
    }



//    df1.orderBy($"t0_trade_date".desc).show()



    spark.close()
  }

  def zf(spark:SparkSession,df1:DataFrame): Unit ={
    df1.createOrReplaceTempView("ta1")
    //          zf1_hc(spark)
    import spark.implicits._
    val res = spark.sql(
      """select
        | stock_code,t0_trade_date,t3_trade_date,t9_trade_date,t4_volume,t8_zt,t9_zt,t10_zt,t11_zt,t12_zt,t4_change_pct+t5_change_pct+t6_change_pct+t7_change_pct+t8_change_pct as t4_8,t9_change_pct,t10_change_pct,t10_change_pct+t11_change_pct as  t1011,t10_change_pct+t11_change_pct+t12_change_pct as t1012
        |from ta1 where
        | t0_sfzt=0 and t1_sfzt=0 and t2_sfzt=0 and t3_sfzt=1 and t4_sfzt=0
        | and t4_volume/t3_volume<3
        | and t4_close<t3_close and t5_close<t3_close and t6_close<t3_close and t7_close<t3_close and t8_close<t3_close
        | and t4_change_pct+t5_change_pct+t6_change_pct+t7_change_pct+t8_change_pct<-7
        | and t8_open>=2 and t8_open <60

        |order by t0_trade_date
        |""".stripMargin)
    res.createOrReplaceTempView("ta2")
    res.show(500)

    val df2 = spark.sql(
      """select *,if(t10_change_pct>1.5,t10_change_pct,if(t10_change_pct<-4,-4,if(t1011>-1,t1011,if(t1011<-2,-2,t1012)))) as q,
        |(1+if(t10_change_pct>1.5,t10_change_pct,if(t10_change_pct<-4,-4,if(t1011>-1,t1011,if(t1011<-2,-2,t1012))))/100) as q2
        |from ta2""".stripMargin).where("q is not null and t4_8>-30 and q2>0.9 and q2<1.3")

    df2.orderBy($"t9_trade_date").show(500)
    df2.createOrReplaceTempView("df2")
    //      spark.sql("select q2s,r,count(1) from (select if(q2<1,-1,1) as q2s,round(t9_change_pct,0) as r from df2) group by q2s,r order by q2s,r").show(300)



    //TODO 胜率 最高值与收盘价比较 查看使用的柱体并确定获取的值是上升还是下降
    val df3 = spark.sql("select *,row_number() over(partition by t9_trade_date order by t9_trade_date) as num from df2").where("num=1")
    println(df2.count())
    println(df2.where("q2>1").count())
    println(df2.where("q2<1").count())
    if(df2.count!=0){
      println(df2.select("q2").as[BigDecimal].collect().reduce(_ * _))
    }

    println(df3.count())
    println(df3.where("q2>1").count())
    println(df3.where("q2<1").count())
    if(df3.count!=0){
      println(df3.select("q2").as[BigDecimal].collect().reduce(_ * _))
    }
  }

}
