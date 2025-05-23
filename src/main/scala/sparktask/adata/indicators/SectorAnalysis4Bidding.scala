package sparktask.adata.indicators

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sparktask.listener.{GlobalProgressListener, ProgressBar}

/**
 * 竞价板块指标分析
 */
object SectorAnalysis4Bidding {
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

    val progressListener = new GlobalProgressListener
    spark.sparkContext.addSparkListener(progressListener)

    // 启动进度条线程
    ProgressBar.show(progressListener)



    // 等待所有任务完成
    progressListener.awaitCompletion()
    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }
}
