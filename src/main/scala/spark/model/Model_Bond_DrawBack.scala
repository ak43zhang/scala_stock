package spark.model

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import spark.model.Model_ConnectDrawBack.create_table
import spark.tools.MysqlProperties

/**
 * 阶段涨停板回撤——选债法
 */
object Model_Bond_DrawBack {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions","10")
      .set("spark.driver.memory","6g")
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
    val properties = MysqlProperties.getMysqlProperties()
    val startm = System.currentTimeMillis()

    create_table(spark:SparkSession,properties: Properties)

    val endm = System.currentTimeMillis()
    println("共耗时："+(endm-startm)/1000+"秒")
    spark.close()
  }
}
