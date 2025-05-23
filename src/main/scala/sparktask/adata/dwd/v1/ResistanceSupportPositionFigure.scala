package sparktask.adata.dwd.v1

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 计算其他相关指标
 * 1.压力位，支撑位  【短期高点，长期低点】
 */
object ResistanceSupportPositionFigure {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.stock_codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "10")
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

    val startm = System.currentTimeMillis()

    val stockData = spark.read.parquet("file:///D:\\gsdata\\gpsj_day_all_hs")
      .select("stock_code","trade_time","open","close","high","low","volume")

    stockData.printSchema()
    // 模拟输入数据（假设字段已存在）
//    val stockData = Seq(
//      ("AAPL", "2023-01-03", 130.0, 133.0, 134.5, 129.8, 1000000),
//      ("AAPL", "2023-01-04", 133.5, 135.2, 136.0, 133.0, 1200000),
//      ("AAPL", "2023-01-05", 135.0, 137.8, 138.5, 134.5, 1500000)
//    ).toDF("stock_code", "trade_time", "open", "close", "high", "low", "volume")

    // 核心计算逻辑
    val resultDF20 = calculateSupportResistance(stockData,10)

    resultDF20.where("stock_code='002824'").orderBy(col("trade_time").desc).show(20,truncate = false)
//    resultDF.repartition(1).write.mode("overwrite")
//      .partitionBy("trade_date_year")
//      .parquet("file:///D:\\gsdata\\gpsj_day_all_hs_newmetrics")

    val endm = System.currentTimeMillis()
    println("共耗时："+(endm-startm)/1000+"秒")
    spark.close()
  }

  def calculateSupportResistance(df: DataFrame,ck_short:Int): DataFrame = {
    // 定义转换函数
    def convertNumber(n: Int): Int = -(n - 1)

    // 定义窗口规范（按股票代码分区，按日期排序）
    //短期20-50
    val windowSpec = Window.partitionBy("stock_code").orderBy("trade_time").rowsBetween(convertNumber(ck_short), 0)  // 20日窗口（包含当前行）
//    val windowSpeclong = Window.partitionBy("stock_code").orderBy("trade_time").rowsBetween(convertNumber(ck_long), 0)
    // ===== 1. 前高前低法（20日窗口） =====
    val withPrevHighLow = df
      .withColumn("prev_high", round(max("high").over(windowSpec),2))
      .withColumn("prev_low", round(min("low").over(windowSpec),2))
      .withColumn("prev_high", round(max("high").over(windowSpec),2))
      .withColumn("prev_low", round(min("low").over(windowSpec),2))

    // ===== 2. 移动平均法（20日、50日均线） =====
    val withMA = withPrevHighLow
      .withColumn("ma20", round(avg("close").over(windowSpec),2))
//      .withColumn("ma50", round(avg("close").over(windowSpeclong),2))

    // ===== 3. 布林带法（20日均线 ± 2倍标准差） =====
    val withBollinger = withMA
      .withColumn("stddev", stddev("close").over(windowSpec))
      .withColumn("bollinger_upper", round(col("ma20") + lit(2) * col("stddev"),2))
      .withColumn("bollinger_lower", round(col("ma20") - lit(2) * col("stddev"),2))

    // ===== 4. 枢轴点法（前一交易日数据） =====
    val withPivot = withBollinger
      .withColumn("prev_day_high", round(lag("high", 1).over(Window.partitionBy("stock_code").orderBy("trade_time")),2))
      .withColumn("prev_day_low", round(lag("low", 1).over(Window.partitionBy("stock_code").orderBy("trade_time")),2))
      .withColumn("prev_day_close", round(lag("close", 1).over(Window.partitionBy("stock_code").orderBy("trade_time")),2))
      .withColumn("pivot_point", round((col("prev_day_high") + col("prev_day_low") + col("prev_day_close")) / 3,2))
      .withColumn("r1", round(col("pivot_point") * 2 - col("prev_day_low"),2))  // 压力位
      .withColumn("s1", round(col("pivot_point") * 2 - col("prev_day_high"),2)) // 支撑位

    // ===== 5. 综合计算压力位和支撑位（取各方法最大值/最小值） =====
    val withCompositeLevels = withPivot
      .withColumn("trade_date_year",substring(col("trade_time"),0,4))
      .withColumn("pressure_level", greatest(
        col("prev_high"),  // 前高法
        col("ma50"),       // 长期均线压力
        col("bollinger_upper"),  // 布林带上轨
        col("r1")          // 枢轴点压力
      ))
      .withColumn("support_level", least(
        col("prev_low"),   // 前低法
        col("ma20"),       // 短期均线支撑
        col("bollinger_lower"),  // 布林带下轨
        col("s1")          // 枢轴点支撑
      ))
      // 清理中间列，保留最终结果
      .select(
        col("stock_code"),
        col("trade_date_year"),
        col("trade_time"),
        col("open"),
        col("close"),
        col("high"),
        col("low"),
        col("volume"),
        col("pressure_level"),
        col("support_level")
//        col("prev_high"),  // 前高法
//        col("ma50"),       // 长期均线压力
//        col("bollinger_upper"),  // 布林带上轨
//        col("r1") ,         // 枢轴点压力
//        col("prev_low"),   // 前低法
//        col("ma20"),       // 短期均线支撑
//        col("bollinger_lower"),  // 布林带下轨
//        col("s1")          // 枢轴点支撑
      )

    // 处理初始窗口的null值（例如前20天数据不足）
    withCompositeLevels.na.fill(Map(
      "pressure_level" -> 0.0,
      "support_level" -> 0.0
    ))
  }
}
