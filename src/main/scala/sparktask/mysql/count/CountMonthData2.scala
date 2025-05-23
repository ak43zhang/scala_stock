package sparktask.mysql.count

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CountMonthData2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.local.dir","D:\\Temp")
    val spark = SparkSession
      .builder()
      .appName("CountMonthData")
      .config(conf)
      .getOrCreate()

     spark.sparkContext.setLogLevel("ERROR")

     val df15 = spark.read.parquet("file:///D:\\gsdata\\gpsj_15days\\trade_date_month=2024-04")

     df15.createTempView("ta2")

     spark.sql("select count(1) from ta2").show
    spark.close()
  }
}
