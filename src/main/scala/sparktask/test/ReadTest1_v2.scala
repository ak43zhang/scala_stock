package sparktask.test

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadTest1_v2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "10")
      .set("spark.driver.memory", "4g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "10")
      .set("spark.sql.parquet.enableVectorizedReader","false")
      .set("spark.local.dir", "D:\\SparkTemp")

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val url = "jdbc:mysql://localhost:3306/gs"
    val driver = "com.mysql.cj.jdbc.Driver"
    val user = "root"
    val pwd = "123456"

    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", pwd)
    properties.setProperty("url", url)
    properties.setProperty("driver", driver)
    val startm = System.currentTimeMillis()

    val setdate = "2025-04-14"
    val setdate_ = setdate.replaceAll("-","")
    val year = setdate.substring(0,4)

    var basequerydf: DataFrame = spark.read.jdbc(url, s"wencaiquery_basequery_$year", properties)
      .where(s"trade_date='$setdate'")
      .select("`代码`", "`简称`")
    //      println("querydf-----------"+querydf.count())
    basequerydf.createOrReplaceTempView("basequery")

    var venturedf: DataFrame = spark.read.jdbc(url, s"wencaiquery_venture_$year", properties)
      .where(s"trade_date='$setdate'")
    //      println("querydf-----------"+querydf.count())
    venturedf.createOrReplaceTempView("venture")

    //TODO 最大涨幅>5%,实体涨幅

    spark.sql(
      """
        |select * from basequery left join (select * from
        |(select q1.`代码` as dm,collect_list(`风险类型`) as fxlxs from basequery as q1 left join venture as q2 on q1.`代码`=q2.`代码` where `风险类型` not like '%屠龙刀%' group by q1.`代码`)
        |order by size(fxlxs) desc) as q3 on basequery.`代码`=q3.dm where q3.dm is null
        |""".stripMargin)
      .select("代码").repartition(1).write.mode("overwrite").option("encoding", "UTF-8")
            .text(s"file:///C:\\Users\\Administrator\\Desktop\\gs2024\\filter_table\\${setdate_}\\关注")

    val venture_df = spark.sql(
      """
        |select * from basequery left join (select * from
        |(select q1.`代码` as dm,collect_list(`风险类型`) as fxlxs from basequery as q1 left join venture as q2 on q1.`代码`=q2.`代码` where `风险类型` not like '%屠龙刀%' group by q1.`代码`)
        |order by size(fxlxs) desc) as q3 on basequery.`代码`=q3.dm where q3.dm is not null
        |""".stripMargin)

    venture_df.select("代码").repartition(1).write.mode("overwrite").option("encoding", "UTF-8")
      .text(s"file:///C:\\Users\\Administrator\\Desktop\\gs2024\\filter_table\\${setdate_}\\风险")

    venture_df.repartition(1).write.mode("overwrite")
      .parquet(s"file:///C:\\Users\\Administrator\\Desktop\\gs2024\\filter_table\\${setdate_}\\风险parquet")





    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }
}
