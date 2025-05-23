package sparktask.Iwencai

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Demo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled","true")
    val spark = SparkSession
      .builder()
      .appName("Demo1")
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.option("inferSchema", "true").option("header","true").csv("file:///C:\\Users\\Administrator\\Desktop\\Table20241108.csv")
//      df.show(false)
    df.printSchema()

    df.createTempView("ta1")

    val df2 = spark.sql("""
      |select `代码`,`名称`,`涨幅`,`实体涨幅`,`最大涨幅`,`量比`,`主力净量`,`主力净额`,`散户数量`,`压力位`,`支撑位`,`开盘价`,`收盘价`,`最高`,`最低`,`昨收`,
      | `压力位`-`昨收`*1.03 as `靠压位`,`昨收`*1.03-`支撑位` as `靠支位`,(ABS(`压力位`-`昨收`*1.03)-ABS(`昨收`*1.03-`支撑位`))/`昨收` as `靠差`
      | from ta1
      | where `主力净额`>1000000 and `最大涨幅`>3 and `量比`>0.9
      |
      | order by `涨幅` desc
      |""".stripMargin)
      df2.show(200,false)

    println(df2.count())

    spark.close()
  }
}
