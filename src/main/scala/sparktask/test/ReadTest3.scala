package sparktask.test

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

object ReadTest3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "10")
      .set("spark.sql.broadcastTimeout","60000")
      .set("spark.driver.memory", "6g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "10")
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

    import spark.implicits._

//    val ta1: DataFrame = spark.read.jdbc(url, "result_expect_risk1", properties)
//    ta1.createTempView("ta1")
//    spark.sql(
//      """
//        | select replace(fxlx,'risk_',''),fxlx,count(1) from (select split(fxlxs,',') as fxlxs2 from ta1) lateral view explode(fxlxs2) as fxlx  group by fxlx order by count(1) desc
//        |""".stripMargin).show(300)


    // ===================================================================================
    val ta1: DataFrame = spark.read.jdbc(url, "analysis_news", properties)
      .where("table_name='news_financial'")
        ta1.createTempView("ta1")

    spark.sql(
      """
        |SELECT
        |  table_name,get_json_object(json_value, '$.消息集合') AS message_array
        |FROM ta1
        |""".stripMargin).createOrReplaceTempView("ta2")

    spark.sql(
      """
        |SELECT
        |  explode(
        |    split(
        |      regexp_replace(
        |        regexp_replace(
        |          message_array,
        |          '^\\[|\\]$',
        |          ''
        |        ),
        |        '\\}\\,\\s*\\{',
        |        '}|||{'
        |      ),
        |      '\\|\\|\\|'
        |    )
        |  ) AS message
        |FROM ta2
        |
        |""".stripMargin).createOrReplaceTempView("ta3")

    spark.sql("select * from ta3").show(false)

    spark.sql(
      """
        |SELECT
        |  get_json_object(message, '$.消息id') AS message_ids
        |FROM ta3
        |
        |""".stripMargin).where("message_ids is not null and message_ids LIKE '%,%'").show(1000,false)

    // ===================================================================================
    val tb1: DataFrame = spark.read.jdbc(url, "analysis_notices", properties)
    tb1.createTempView("tb1")

    spark.sql(
      """
        |SELECT
        |    get_json_object(json_value, '$.公告集合') AS array_str
        |  FROM tb1
        |""".stripMargin).createOrReplaceTempView("tb2")

    spark.sql(
      """
        |SELECT
        |    explode(
        |      split(
        |        regexp_replace(
        |          regexp_replace(
        |            array_str,
        |            '^\\[|\\]$',
        |            ''
        |          ),
        |          '\\}\\,\\s*\\{',
        |          '}|||{'
        |        ),
        |        '\\|\\|\\|'
        |      )
        |    ) AS announcement
        |  FROM tb2
        |
        |""".stripMargin).createOrReplaceTempView("tb3")

    spark.sql(
      """
        |SELECT
        |  get_json_object(announcement, '$.公告id') AS announcement_id
        |FROM tb3
        |""".stripMargin)
//      .where("announcement_id is not null and announcement_id LIKE '%,%'")
      .show(1000,false)

//    val ta1: DataFrame = spark.read.jdbc(url, "tld_filter", properties)
//      .where("trade_date > '2023-01-01'")
////      .where("trade_date between '2023-04-01' and '2024-09-01'")
//    ta1.persist(StorageLevel.MEMORY_AND_DISK_SER)
//    ta1.createTempView("ta1")
//
//    val ta2: DataFrame = spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_20days\\trade_date_month=20[18,19,20,21,22,23,24,25]*")
//        .select("stock_code","t0_trade_date","t0_sfzt","t1_sfzt","t0_kpzf","t0_zgzf","t0_zdzf","t0_spzf","t1_kpzf","t1_zgzf","t1_zdzf","t1_spzf")
//    ta2.persist(StorageLevel.MEMORY_AND_DISK_SER)
//    ta2.createOrReplaceTempView("ta2")
//
//    val ta3 = spark.read.parquet("file:///D:\\gsdata\\pressure_support_calculator\\valid_results_pressure_advanced_dip_strategy")
//    //        .where("trade_time between '2020-01-01' and '2022-01-01'")
//    ta3.persist(StorageLevel.MEMORY_AND_DISK_SER)
//    ta3.createOrReplaceTempView("ta3")
//
//          val middf1 = spark.sql(
//            s"""
//               |select dm,`简称`,trade_date,t0_sfzt,t1_sfzt,
//               |t0_kpzf,t0_zgzf,t0_zdzf,t0_spzf,
//               |t1_kpzf,t1_zgzf,t1_zdzf,t1_spzf
//               |
//               |from ta2 left join ta1 on ta1.dm=ta2.stock_code and ta1.trade_date=ta2.t0_trade_date
//               |
//               |where dm is not null and t0_zgzf>=8
//               |order by trade_date
//               |""".stripMargin)
////            .where(s"row<=$row")
//            .distinct()
//                middf1.show(100000)


//    val ta4: DataFrame = spark.read.parquet("file:///D:\\gsdata\\gpsj_day_all_hs\\trade_date_month=2025-03")
//    ta4.createOrReplaceTempView("ta4")
//    spark.sql("select trade_date,count(1) from ta4 group by trade_date order by trade_date desc").show()




    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }
}
