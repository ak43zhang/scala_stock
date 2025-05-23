package sparktask.adata.indicators

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * 历史指数指标分析
 */
object LhbAnalysis4History {
  def main(args: Array[String]): Unit = {
    val startm = System.currentTimeMillis()
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "10")
      .set("spark.sql.broadcastTimeout","60000")
      .set("spark.driver.memory", "8g")
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

//    val gpdf: DataFrame = spark.read.parquet("file:///D:\\gsdata\\gpsj_day_all_hs")
//    gpdf.createOrReplaceTempView("gpdf")
    createTable(spark, url, properties)

    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }

  /**
   * 龙虎榜红票分组统计
   */
  def lhb_distribution(spark: SparkSession, beforedate:String): Unit ={
      spark.sql(
        """
          |select * from data_lhb_history2 where
          |""".stripMargin).show()
  }

  /**
   * 数据统一生成表
   *
   * @param spark
   * @param url
   * @param properties
   */
  def createTable(spark: SparkSession, url: String, properties: Properties): Unit = {
    val jyrldf: DataFrame = spark.read.jdbc(url, "data_jyrl", properties)
    jyrldf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    jyrldf.createOrReplaceTempView("data_jyrl")

    val lhbdf: DataFrame = spark.read.jdbc(url, "data_lhb_history2", properties)
    lhbdf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    lhbdf.createOrReplaceTempView("data_lhb_history2")




  }

}
