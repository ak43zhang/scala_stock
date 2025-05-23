package sparktask.mysql.update.demotest

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 基础参数调整
 */
object BasicParameterAdjustment {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.local.dir","D:\\Temp")
    val spark = SparkSession
      .builder()
      .appName("BasicParameterAdjustment")
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    spark.close()
  }
}
