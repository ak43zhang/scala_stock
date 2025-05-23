package sparktask.test

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

/**
 * 工业级筹码集中度分析系统V2
 * 核心指标：
 * 1. price_level        - 筹码价格区间（精度0.01元）
 * 2. concentration_ratio - 该价位筹码集中度（0.0-1.0）
 * 3. cumulative_volume   - 累计成交量（股）
 * 4. ma_cost             - 移动平均成本价（精度0.01元）
 *
 * 工业级特性：
 * 1. 分布式精确计算
 * 2. 内存优化管理
 * 3. 数据异常防护
 * 4. 动态窗口调整
 */
object AdvancedChipAnalysis {

  // 生产环境配置常量
  private val PRICE_PRECISION = 2
  private val PRICE_STEP = 0.01
  private val ANALYSIS_DAYS = 30
  private val PRECISION_FACTOR = math.pow(10, PRICE_PRECISION).toInt

  def main(args: Array[String]): Unit = {
    val startm = System.currentTimeMillis()

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("PressureSupportCalculator")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.shuffle.partitions", "200")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    try {
      val inputPath = "file:///D:\\gsdata\\gpsj_day_all_hs\\trade_date_month=2025*"
      val outputPath = "file:///D:\\gsdata\\AdvancedChipAnalysis"

      val rawDF = loadAndSanitizeData(spark, inputPath)
      rawDF.show()
      val processedDF = processPriceDistribution(rawDF)
      processedDF.show()
      val resultDF = calculateMetrics(processedDF, ANALYSIS_DAYS)
      resultDF.show()
//      resultDF.write.mode("overwrite").parquet(outputPath)
    } finally {
      spark.stop()
    }
  }

  /**
   * 数据清洗与消毒
   * 工业级数据校验：
   * 1. 价格有效性（高>=低）
   * 2. 成交量非负
   * 3. 日期格式标准化
   * 4. 极端值过滤
   */
  def loadAndSanitizeData(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._

    spark.read.parquet(path).where("stock_code='002253'")
      .filter(
        $"stock_code".isNotNull &&
          $"trade_date".isNotNull &&
          $"volume" > 0 &&
          $"high" >= $"low" &&
          $"low" > 0 &&  // 防止负价格
          $"high" < 100000 && // 过滤异常高价
          $"volume" < 1e12 // 过滤异常成交量
      )
//      .withColumn("date", to_date($"trade_date", "yyyyMMdd"))
//      .drop("trade_date")
  }

  /**
   * 价格分布处理核心逻辑
   * 关键技术点：
   * 1. 动态价格区间生成
   * 2. 精度补偿算法
   * 3. 内存优化持久化
   */
  def processPriceDistribution(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    // 安全价格区间表达式（修复括号问题）
    val priceRangeExpr = s"""
    transform(
      sequence(
        cast(round(least(low, high), $PRICE_PRECISION) * $PRECISION_FACTOR as int),
        cast(round(greatest(low, high), $PRICE_PRECISION) * $PRECISION_FACTOR as int),
        cast($PRICE_STEP * $PRECISION_FACTOR as int)
      ),
      x -> round(x / ${PRECISION_FACTOR}D, $PRICE_PRECISION)
    )
  """

    val processed = df
      .withColumn("price_range",
        when($"high" === $"low", array(round($"high", PRICE_PRECISION)))
          .otherwise(expr(priceRangeExpr))
      )
      .select(
        $"stock_code",
        $"trade_date",
        explode($"price_range").as("price_level"),
        $"volume"
      )
      .groupBy($"stock_code", $"trade_date", $"price_level")
      .agg(sum($"volume").as("daily_volume"))

    processed.persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

  /**
   * 指标计算引擎
   * 计算以下指标：
   * 1. 累计成交量（ANALYSIS_DAYS窗口）
   * 2. 筹码集中度（该价位成交量/窗口期总成交量）
   * 3. 移动平均成本价（成交量加权平均）
   */
  def calculateMetrics(df: DataFrame, windowDays: Int): DataFrame = {
    import df.sparkSession.implicits._
    // 窗口定义（按证券+价格分区，按时间排序）
    val priceWindow = Window.partitionBy($"stock_code", $"price_level")
      .orderBy($"trade_date")
      .rowsBetween(-windowDays, 0)

    // 公共表达式避免重复计算
    val cumulativeVolume = sum($"daily_volume").over(priceWindow)
    val totalVolumeExpr = sum(cumulativeVolume).over(Window.partitionBy($"stock_code", $"trade_date"))

    df.withColumn("cumulative_volume", cumulativeVolume)
      .filter($"cumulative_volume" > 0)  // 过滤无交易数据
      .withColumn("total_volume", totalVolumeExpr)
      .withColumn("concentration_ratio",
        round($"cumulative_volume" / $"total_volume", 4))
      .withColumn("ma_cost",
        round(
          sum($"price_level" * $"cumulative_volume").over(priceWindow) /
            sum($"cumulative_volume").over(priceWindow),
          PRICE_PRECISION
        ))
      .select(
        $"stock_code",
        $"trade_date",
        $"price_level",
        $"concentration_ratio",
        $"cumulative_volume",
        $"ma_cost"
      )
      .orderBy($"stock_code", $"trade_date", $"price_level")
  }
}