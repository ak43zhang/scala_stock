package sparktask.Iwencai.condition

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sparktask.Iwencai.condition.ConditionGeneration2.midConditionGeneration2
import sparktask.Iwencai.condition.ConditionGeneration3.condition2result

object Summarize {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions","10")
      .set("spark.driver.memory","8g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "10")
      .set("spark.local.dir","D:\\SparkTemp")

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

    val date = "2025-01-27"

    ConditionGeneration2.createTableView(spark)
    midConditionGeneration2(spark,date)
    condition2result(spark,date)


    val endm = System.currentTimeMillis()
    println("共耗时："+(endm-startm)/1000+"秒")
    spark.close()
  }
}
