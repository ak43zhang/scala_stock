package sparktask.mysql

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object Getgg2txt {
  def main(args: Array[String]): Unit = {
    val startm = System.currentTimeMillis()
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "20")
      .set("spark.sql.broadcastTimeout","60000")
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

    /**
     * setdate：设置当天盯盘时间
     * beforedate：取盯盘前一交易日
     * before10date:取盯盘前10个交易日
     */
    //    val setdate = "20250307"
    //    val formattedDate = s"${setdate.take(4)}-${setdate.slice(4, 6)}-${setdate.takeRight(2)}"

    val newsdf: DataFrame = spark.read.jdbc(url, "news", properties)
    newsdf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    newsdf.createOrReplaceTempView("news")

    val cls_newsdf: DataFrame = spark.read.jdbc(url, "cls_news", properties)
    cls_newsdf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    cls_newsdf.createOrReplaceTempView("cls_news")

    val combine_newsdf: DataFrame = spark.read.jdbc(url, "combine_news", properties)
    combine_newsdf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    combine_newsdf.createOrReplaceTempView("combine_news")

    // 获取当前日期
    val today: LocalDate = LocalDate.now()
    // 定义格式化器
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    // 格式化为字符串
    val nowdateStr: String = today.format(formatter)
    val start_time = LocalDate.now().minusDays(1).format(formatter)
    val end_time = LocalDate.now().minusDays(-1).format(formatter)
    //    val start_time = "2025-03-27"
//    val end_time = "2025-03-29"

    spark.sql(
      s"""
        |(select `内容` as nr from news where `时间` between '$start_time' and '$end_time')
        |union
        |(select `内容` as nr from cls_news where `发布时间` between '$start_time' and '$end_time')
        |union
        |(select `内容` as nr from combine_news where `发布时间` between '$start_time' and '$end_time')
        |""".stripMargin).repartition(1).write.mode("overwrite").text(s"file:///C:\\Users\\Administrator\\Desktop\\gs2024\\news\\$nowdateStr")

    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }
}
