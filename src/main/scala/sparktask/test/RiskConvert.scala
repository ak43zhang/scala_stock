package sparktask.test

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import sparktask.tools.MysqlTools

/**
 * 风险数据转换
 */
object RiskConvert {
  def main(args: Array[String]): Unit = {
    val startm = System.currentTimeMillis()
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "20")
      .set("spark.sql.broadcastTimeout", "60000")
      .set("spark.driver.memory", "8g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "20")
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


    val jyrldf: DataFrame = spark.read.jdbc(url, "data_jyrl", properties)
    jyrldf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    jyrldf.createOrReplaceTempView("data_jyrl")

    val listsetDate = spark.sql("select trade_date from data_jyrl where  trade_date between '2014-01-01' and '2024-08-01' and trade_status='1' order by trade_date desc")
      .collect()
      .map(f => f.getAs[String]("trade_date")).toList

    for (setday <- listsetDate) {
      val set_date = setday.replaceAll("-", "")
      val set_year = setday.substring(0, 4)
      val input_table_name = s"wencaiquery_venture_$set_date"
      val output_table_name = s"wencaiquery_venture_$set_year"
//      println(setday)
//      println(set_year)
//      println(input_table_name)
//      println(output_table_name)
      var querydf: DataFrame = spark.read.jdbc(url, input_table_name, properties)
      //      println("querydf-----------"+querydf.count())
      querydf.createOrReplaceTempView("venture")

      val df2 = spark.sql(
        s"""
           |select *,'$setday' as trade_date from venture where `风险类型` in ('立案调查','分红派息','监管日期','流动性风险_换手','流动性风险_换手2','融资余额风险')
           |""".stripMargin)

      if (df2.count() == 0) {
        println("=======================================此时间没有数据："+setday)
      }

      //删除日期内的条件数据并重写
      try {
        // 通过 Spark 执行 SQL 删除语句
        val deleteQuery = s"DELETE FROM $output_table_name WHERE `风险类型` in ('立案调查','分红派息','监管日期','流动性风险_换手','流动性风险_换手2','融资余额风险') and trade_date='$setday'"
        MysqlTools.mysqlEx(output_table_name, deleteQuery)
//        println("数据删除成功！")
      } catch {
        case e: Exception => println(s"数据删除失败: ${e.getMessage}")
      }

      df2.write.mode("append").jdbc(url, output_table_name, properties)

    }


    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }
}
