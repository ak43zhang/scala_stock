package sparktask.adata.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * 通过条件分析A股选择胜率
 */
object Spark2AnsCondition {
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

    //基础数据表
    val basedata = spark.read.option("header",true).option("sep", "\t").csv("file:///D:\\gsdata\\基础数据表.csv")
      .withColumn("代码",substring(col("代码"), 3, 6))
      .toDF("dm","mc")
//    basedata.show()
    basedata.createTempView("ta2")

    //TODO
    val df = spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_20days\\trade_date_month=2024-11")
    df.createTempView("ta1")

    val df3 = spark.sql(
      """
        |select stock_code,t10_trade_date,
        |t0_spzf + t1_spzf + t2_spzf + t3_spzf + t4_spzf + t5_spzf + t6_spzf + t7_spzf + t8_spzf + t9_spzf  as qsrqjzf,
        |t11_spzf as d1,t11_kxzt,
        |t11_spzf+t12_spzf as d2,t12_kxzt,
        |t11_spzf+t12_spzf+t13_spzf as d3,t13_kxzt,
        |t11_spzf+t12_spzf+t13_spzf+t14_spzf as d4,t14_kxzt,
        |t11_spzf+t12_spzf+t13_spzf+t14_spzf+t15_spzf as d5,t15_kxzt,
        |t11_spzf+t12_spzf+t13_spzf+t14_spzf+t15_spzf+t16_spzf as d6,t16_kxzt,
        |t11_spzf+t12_spzf+t13_spzf+t14_spzf+t15_spzf+t16_spzf+t17_spzf as d7,t17_kxzt,
        |t11_spzf+t12_spzf+t13_spzf+t14_spzf+t15_spzf+t16_spzf+t17_spzf+t18_spzf as d8,t18_kxzt,
        |t11_spzf+t12_spzf+t13_spzf+t14_spzf+t15_spzf+t16_spzf+t17_spzf+t18_spzf+t19_spzf as d9,t19_kxzt
        |from ta1 where stock_code in (select dm from ta2) and (t10_sfzt=1 and t10_trade_date between '2024-11-18' and '2024-12-16') and t7_sfzt=0 and t8_sfzt=0 and t9_sfzt=0
        |order by t10_trade_date
        |""".stripMargin)
    df3.createTempView("ta3")
//      df3.show(1000)
    println(df3.count)

    spark.sql(
      """
        |select *,
        | case when d2<-6 and t12_kxzt in ('红柱下影线' ) then 2
        |      when d3<-6 and t13_kxzt in ('红柱下影线' ) then 3
        |      when d4<-6 and t14_kxzt in ('红柱下影线' ) then 4
        |      when d5<-6 and t15_kxzt in ('红柱下影线' ) then 5
        |      when d6<-6 and t16_kxzt in ('红柱下影线' ) then 6
        |      when d7<-6 and t17_kxzt in ('红柱下影线' ) then 7
        |      when d8<-6 and t18_kxzt in ('红柱下影线' ) then 8
        |      when d9<-6 and t19_kxzt in ('红柱下影线' ) then 9
        |else 0
        |end as bj from ta3 ORDER BY t10_trade_date
        |""".stripMargin).filter("bj!=0").show(1000)


    val endm = System.currentTimeMillis()
    println("共耗时："+(endm-startm)/1000+"秒")
    spark.close()
  }
}
