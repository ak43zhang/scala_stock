package spark.check

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataCheck {
  def main(args: Array[String]): Unit = {
    // 创建Spark环境（生产级配置）
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      //      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.shuffle.partitions", "10")
      .config("spark.driver.memory","8g")
      // 增加JDBC并行任务数
      .config("spark.jdbc.parallelism", "10")
      .config("spark.local.dir","D:\\SparkTemp")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  // 高效序列化
      .getOrCreate()

    val url = "jdbc:mysql://localhost:3306/gs"
    val driver = "com.mysql.cj.jdbc.Driver"
    val user = "root"
    val pwd = "123456"

    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", pwd)
    properties.setProperty("url", url)
    properties.setProperty("driver", driver)

    spark.sparkContext.setLogLevel("ERROR")  // 屏蔽非关键日志

    spark.read.parquet(s"file:///D:\\gsdata2\\gpsj_day_all_hs").orderBy(col("trade_date").desc).show()
  }
}
