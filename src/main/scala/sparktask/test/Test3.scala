package sparktask.test

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
 *
    字段名	                            计算逻辑	                                         说明
prev_close	              lag(close, 1)	                                    使用窗口函数获取前一日收盘价
open_change_rate	        (open - prev_close) / prev_close * 100	          开盘相对于昨日收盘的涨跌幅
close_change_rate	        (close - prev_close) / prev_close * 100	          收盘相对于昨日收盘的涨跌幅
high_change_rate	        (high - prev_close) / prev_close * 100	          最高价相对于昨日收盘的涨跌幅
low_change_rate	          (low - prev_close) / prev_close * 100	            最低价相对于昨日收盘的涨跌幅
change_amount	            close - prev_close	                              今日收盘与昨日收盘的价差
change_rate	              (close - prev_close) / prev_close * 100	          今日涨跌幅，同close_change_rate
body_change_rate	        (close - open) / prev_close * 100	                K线实体相对于昨日收盘的幅度
range_change_rate	        (high - low) / prev_close * 100	                  当日价格区间相对于昨日收盘的幅度

 */
object Test3 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "10")
      .set("spark.sql.broadcastTimeout","60000")
      .set("spark.driver.memory", "6g")
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
    val startm = System.currentTimeMillis()

    gn_bk(spark,url,properties)


    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
  }

  def gn_bk(spark:SparkSession,url:String,properties: Properties): DataFrame ={
    var df:DataFrame = spark.read.jdbc(url, "ths_gn_bk", properties)
      .withColumn("open", col("开盘价").cast(DecimalType(10, 2)))
      .withColumn("close", col("收盘价").cast(DecimalType(10, 2)))
      .withColumn("high", col("最高价").cast(DecimalType(10, 2)))
      .withColumn("low", col("最低价").cast(DecimalType(10, 2)))
      .withColumn("volume", col("成交量").cast(IntegerType))
      .withColumn("amount", col("成交额").cast(DecimalType(20, 2)))
      .withColumn("trade_date", col("日期"))

    //      .createOrReplaceTempView("ta1")
    //按月保存每日数据

    import spark.implicits._

    // 定义窗口函数 - 按股票概念分组，按交易日期排序
    val windowSpec = Window
      .partitionBy($"code")  // 按概念代码分组
      .orderBy($"trade_date")        // 按交易日期排序

    val windowSpecLag1 = Window
      .partitionBy($"code")
      .orderBy($"trade_date")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val df2 = df
      // 1. 计算昨日收盘价
      .withColumn("pre_close",
        lag($"close", 1).over(windowSpec)
          .alias("pre_close")  // 获取前一天的收盘价
      )
      // 2. 计算开盘涨幅 (%) - (今日开盘 - 昨日收盘) / 昨日收盘 * 100
      .withColumn("kpzf",
        when($"pre_close".isNotNull && $"pre_close" =!= 0,
          round((($"open" - $"pre_close") / $"pre_close") * 100, 2)
        ).otherwise(lit(null))
          .alias("kpzf")
      )
      // 3. 计算收盘涨幅 (%) - (今日收盘 - 昨日收盘) / 昨日收盘 * 100
      .withColumn("spzf",
        when($"pre_close".isNotNull && $"pre_close" =!= 0,
          round((($"close" - $"pre_close") / $"pre_close") * 100, 2)
        ).otherwise(lit(null))
          .alias("spzf")
      )
      // 4. 计算最高涨幅 (%) - (今日最高 - 昨日收盘) / 昨日收盘 * 100
      .withColumn("zgzf",
        when($"pre_close".isNotNull && $"pre_close" =!= 0,
          round((($"high" - $"pre_close") / $"pre_close") * 100, 2)
        ).otherwise(lit(null))
          .alias("zgzf")
      )
      // 5. 计算最低涨幅 (%) - (今日最低 - 昨日收盘) / 昨日收盘 * 100
      .withColumn("zdzf",
        when($"pre_close".isNotNull && $"pre_close" =!= 0,
          round((($"low" - $"pre_close") / $"pre_close") * 100, 2)
        ).otherwise(lit(null))
          .alias("zdzf")
      )
      // 6. 计算涨跌额 - 今日收盘 - 昨日收盘
      .withColumn("change",
        when($"pre_close".isNotNull,
          round($"close" - $"pre_close", 2)
        ).otherwise(lit(null))
          .alias("change")
      )

      // 7. 计算涨跌幅 (%) - 同收盘涨幅，但保留字段别名
      .withColumn("change_pct",
        when($"pre_close".isNotNull && $"pre_close" =!= 0,
          round((($"close" - $"pre_close") / $"pre_close") * 100, 2)
        ).otherwise(lit(null))
          .alias("change_pct")
      )
      // 8. 计算实体涨幅 (%) - (今日收盘 - 今日开盘) / 昨日收盘 * 100
      .withColumn("stzf",
        when($"pre_close".isNotNull && $"pre_close" =!= 0,
          round((($"close" - $"open") / $"pre_close") * 100, 2)
        ).otherwise(lit(null))
          .alias("stzf")
      )
      // 9. 计算区间涨幅 (%) - (今日最高 - 今日最低) / 昨日收盘 * 100
      .withColumn("qjzf",
        when($"pre_close".isNotNull && $"pre_close" =!= 0,
          round((($"high" - $"low") / $"pre_close") * 100, 2)
        ).otherwise(lit(null))
          .alias("qjzf")
      )
      .select("name","code","trade_date","open","close","high","low","change","change_pct","volume","amount","pre_close","kpzf","spzf","zgzf","zdzf","stzf","qjzf")

    df2.show()
    df2
  }

}
