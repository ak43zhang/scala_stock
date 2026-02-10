package spark.bus.sector

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import spark.tools.MysqlProperties
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
 * kpzf（开盘涨幅）：(开盘价 - 前一日收盘价) / 前一日收盘价 × 100
 * spzf（收盘涨幅）：(收盘价 - 前一日收盘价) / 前一日收盘价 × 100
 * zgzf（最高涨幅）：(最高价 - 前一日收盘价) / 前一日收盘价 × 100
 * zdzf（最低涨幅）：(最低价 - 前一日收盘价) / 前一日收盘价 × 100
 * qjzf（区间涨幅）：(收盘价 - 开盘价) / 前一日收盘价 × 100
 * stzf（实体涨幅）：(收盘价 - 开盘价) / 开盘价 × 100
 */
object sector_ods {
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

    val properties = MysqlProperties.getMysqlProperties()
    val url = properties.getProperty("url")

    val startm = System.currentTimeMillis()

    val table_name = "ths_gn_bk"
    val table_name_ods = "ths_gn_bk_ods"

    val df = spark.read.jdbc(url,table_name,properties)

    // 1. 重命名字段为英文
    val renamedDF = df
      .withColumnRenamed("日期", "trade_date")
      .withColumnRenamed("开盘价", "open")
      .withColumnRenamed("最高价", "high")
      .withColumnRenamed("最低价", "low")
      .withColumnRenamed("收盘价", "close")
      .withColumnRenamed("成交量", "volume")
      .withColumnRenamed("成交额", "amount")
      // name和code字段保持不变
      .withColumnRenamed("name", "name")
      .withColumnRenamed("code", "code")

    // 2. 确保按日期排序
    val dfSorted = renamedDF.orderBy("code", "trade_date")

    // 3. 定义窗口规范
    val windowSpec = Window
      .partitionBy("code")
      .orderBy("trade_date")

    // 4. 计算前一天的收盘价
    val dfWithPrevClose = dfSorted.withColumn(
      "prev_close",
      lag("close", 1).over(windowSpec)
    )

    // 5. 计算所有涨幅指标（保留4位小数）
    val resultDF = dfWithPrevClose
      .withColumn("kpzf",
        when(col("prev_close").isNotNull && col("prev_close") =!= 0,
          round((col("open") - col("prev_close")) / col("prev_close") * 100, 4))
          .otherwise(lit(null)))
      .withColumn("spzf",
        when(col("prev_close").isNotNull && col("prev_close") =!= 0,
          round((col("close") - col("prev_close")) / col("prev_close") * 100, 4))
          .otherwise(lit(null)))
      .withColumn("zgzf",
        when(col("prev_close").isNotNull && col("prev_close") =!= 0,
          round((col("high") - col("prev_close")) / col("prev_close") * 100, 4))
          .otherwise(lit(null)))
      .withColumn("zdzf",
        when(col("prev_close").isNotNull && col("prev_close") =!= 0,
          round((col("low") - col("prev_close")) / col("prev_close") * 100, 4))
          .otherwise(lit(null)))
      .withColumn("qjzf",
        when(col("prev_close").isNotNull && col("prev_close") =!= 0,
          round((col("close") - col("open")) / col("prev_close") * 100, 4))
          .otherwise(lit(null)))
      .withColumn("stzf",
        when(col("open").isNotNull && col("open") =!= 0,
          round((col("close") - col("open")) / col("open") * 100, 4))
          .otherwise(lit(null)))
      .drop("prev_close")

    // 6. 显示结果
    println("\n计算结果：")
    resultDF.show(false)

    resultDF.write.mode("overwrite").jdbc(url,table_name_ods,properties)

    val endm = System.currentTimeMillis()
    println("共耗时："+(endm-startm)/1000+"秒")
    spark.close()
  }
}
