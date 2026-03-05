package sparktask.test

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 *
 */
object Test5 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "400")
      .set("spark.sql.broadcastTimeout","60000")
      .set("spark.driver.memory", "12g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "10")
      .set("spark.local.dir", "D:\\SparkTemp")

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val url = "jdbc:mysql://192.168.0.100:3306/gs"
    val driver = "com.mysql.cj.jdbc.Driver"
    val user = "root"
    val pwd = "123456"

    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", pwd)
    properties.setProperty("url", url)
    properties.setProperty("driver", driver)
    val startm = System.currentTimeMillis()

    // 尝试解决字典编码兼容性问题
//    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
//    spark.conf.set("spark.sql.parquet.filterPushdown", "false")
//    // 设置输出压缩为 snappy
//    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
//    val data_df = spark.read.parquet(s"file:///D:\\gsdata2\\gpsj_day_all_hs")
////    data_df.show()
//    data_df.write
//      .mode("overwrite")          // 按需选择模式
//      .parquet(s"file:///D:\\gsdata2\\gpsj_day_all_hs_snappy")

    spark.read.parquet("file:///D:\\gsdata2\\gpsj_day_all_hs\\trade_date_month=2026-02").where("trade_date='2026-02-27'").show()



    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
  }
  


}
