package sparktask.adata.statistics

import java.util.{Date, Properties}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import sparktask.tools.MysqlTools
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
 * 输出靠近压力位的风险数据到mysql风险表，并筛选出没有风险的数据和有风险的数据保存成txt文件
 */
object Fx002_v2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "4")
      .set("spark.driver.memory", "6g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "4")
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

    val savetablename = "fx002_v5"

    var df: DataFrame = spark.read.jdbc(url, "data_jyrl", properties)
    df.createTempView("data_jyrl")
    val listsetDate = spark.sql("select trade_date from data_jyrl where  trade_date between '2018-01-01' and '2025-02-20' and trade_status='1' order by trade_date desc")
      .collect()
      .map(f => f.getAs[String]("trade_date").replaceAll("-", ""))

    val df3 = spark.read.parquet("file:///D:\\gsdata\\pressure_support_calculator\\valid_results_pressure_advanced_dip_strategy")
    df3.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //        .where("stock_code='000063'")
    df3.createTempView("ta3")

    val df5 = spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_10days\\trade_date_month=20[17,18,19,20,21,22,23,24,25]*")
      .where("t1_zgzf>=3")
    df5.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val df5columns = df5.columns
      .filterNot(c => c.contains("t3_") || c.contains("t4_") || c.contains("t5_") || c.contains("t6_") || c.contains("t7_") || c.contains("t8_") || c.contains("t9_")).dropWhile(p => p.equals("stock_code")).mkString(",")

    df5.createTempView("ta5")

    for (setdate <- listsetDate) {
      var df: DataFrame = spark.read.jdbc(url, s"wencaiquery_basequery_$setdate", properties).select("`代码`", "`简称`").distinct()
      df.createTempView("ta1")


      // 找出当前设置时间的前一个交易时间
      val formattedDate = s"${setdate.take(4)}-${setdate.slice(4, 6)}-${setdate.takeRight(2)}"
      val beforedate: String = spark.sql(s"select trade_time from ta3 where trade_time<'$formattedDate' group by trade_time order by trade_time desc limit 1").head().getString(0)
      println(beforedate)
      // 筛选出前一天的风险数据
      val df4 = spark.sql(s"select stock_code,trade_time,windowSize,close,composite_support,composite_pressure,support_ratio,pressure_ratio,support_types,pressure_types,channel_position from ta3 where trade_time='$beforedate'  ")
      df4.createTempView("ta4")

      val df6: DataFrame = spark.sql(
        s"""
           |select ta4.*,$df5columns
           | from ta4 left join ta5 on ta4.stock_code=ta5.stock_code and ta4.trade_time=ta5.t0_trade_date where ta5.stock_code is not null
           |""".stripMargin)
      df6.createTempView("ta6")

      val df7 = spark.sql("select ta6.* from ta6 left join ta1 on ta6.stock_code=ta1.`代码` where ta1.`代码` is not null")
      df7.createTempView("ta7")

      //过滤非压力位风险数据
      var df8: DataFrame = spark.read.jdbc(url, s"wencaiquery_venture_$setdate", properties).where("`风险类型`!='靠近压力位'").select("`代码`").distinct()
      df8.createTempView("ta8")

      val resdf = spark.sql("select ta7.* from ta7 left join ta8 on ta7.stock_code=ta8.`代码` where ta8.`代码` is null")

      // 执行删除操作
      try {
        // 通过 Spark 执行 SQL 删除语句
        // 编写 SQL 删除语句
        val deleteQuery = s"DELETE FROM $savetablename WHERE t1_trade_date='$formattedDate'"
        MysqlTools.mysqlEx(savetablename, deleteQuery)
        println("数据删除成功！")
      } catch {
        case e: Exception => println(s"数据删除失败: ${e.getMessage}")
      }

      resdf.write.mode("append").jdbc(url, savetablename, properties)

      //      println(resdf.count())
      //      resdf.show(100)


      spark.catalog.dropTempView("ta1")
//      spark.catalog.dropTempView("ta5")
//      spark.catalog.dropTempView("ta3")
      spark.catalog.dropTempView("ta4")
      spark.catalog.dropTempView("ta6")
      spark.catalog.dropTempView("ta7")
      spark.catalog.dropTempView("ta8")
      println(s"当前时间: " + new Date)
    }

    df3.unpersist()
    df5.unpersist()


    spark.close()
  }


}
