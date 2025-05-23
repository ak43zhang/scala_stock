package sparktask.test
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

/**
 * 工业级筹码集中度分析系统
 * 功能特性：
 * 1. 多维度筹码分布计算
 * 2. 动态价格区间划分
 * 3. 滚动窗口分析
 * 4. 内存优化处理
 * 5. 异常数据过滤
 * 6. 分布式计算优化
 *
 * 输入数据schema：
 * - stock_code: String   // 股票代码
 * - trade_date: String   // 交易日期(yyyyMMdd)
 * - open: Double         // 开盘价
 * - close: Double        // 收盘价
 * - high: Double         // 最高价
 * - low: Double          // 最低价
 * - volume: Long         // 成交量
 */
object ChipConcentrationAnalysis2 {

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
      // 参数配置（生产环境应使用配置管理工具）
      val inputPath = "file:///D:\\gsdata\\gpsj_day_all_hs\\trade_date_month=2025*"
      val outputPath = "file:///D:\\gsdata\\chip_concentration_analysis_2"
      val analysisDays = 30 // 分析窗口天数

      // 数据加载与校验
      val rawDF = loadAndValidateData(spark, inputPath)
      rawDF.show()

      // 数据预处理
      val processedDF = preprocessData(rawDF)
      processedDF.show()

      // 计算筹码集中度
      val resultDF = calculateConcentration(processedDF, analysisDays)
      resultDF.show()

      // 结果存储
//      resultDF.write.mode("overwrite").parquet(outputPath)

    } finally {
      spark.stop()
    }
  }

  /**
   * 数据加载与校验
   * 工业级数据质量检查：
   * 1. 空值检查
   * 2. 价格有效性检查
   * 3. 交易量合理性检查
   * 4. 日期格式验证
   */
  def loadAndValidateData(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._

    spark.read.parquet(path)
      .filter(
        // 数据有效性过滤
        $"stock_code".isNotNull &&
          $"trade_date".isNotNull &&
          $"volume" > 0 &&
          $"high" >= $"low" &&
          $"open".between($"low", $"high") &&
          $"close".between($"low", $"high")
      ).where("stock_code='002253'")
//      .withColumn("trade_date", to_date($"trade_date", "yyyyMMdd")) // 日期标准化
  }

  /**
   * 数据预处理：
   * 1. 价格标准化
   * 2. 生成价格区间
   * 3. 计算每日价格分布
   * 4. 内存优化持久化
   */
  def preprocessData(rawDF: DataFrame): DataFrame = {
    import rawDF.sparkSession.implicits._
    val pricePrecision = 2 // 保持两位小数

    val processed = rawDF
      .withColumn("avg_price", round(($"high" + $"low" + $"close") / 3, pricePrecision))
      .withColumn("price_range",
        when($"high" === $"low", array(round($"high", pricePrecision)))
          // 处理浮点数sequence的兼容方案
          .otherwise(
            expr(s"""
            transform(
              sequence(
                cast(round(low, $pricePrecision) * 100 as int),
                cast(round(high, $pricePrecision) * 100 as int),
                1
              ),
              x -> x / 100.0
            )
          """)
          )
      )
      .select(
        $"stock_code",
        $"trade_date",
        $"volume",
        explode($"price_range").as("price_level")
      )
      .groupBy($"stock_code", $"trade_date", $"price_level")
      .agg(sum($"volume").as("daily_volume"))

    processed.persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

  /**
   * 筹码集中度计算核心逻辑
   * 采用动态价格区间算法：
   * 1. 计算N日价格分布
   * 2. 计算累积成交量分布
   * 3. 计算集中度指标
   *
   * @param analysisDays 分析窗口天数
   */
  def calculateConcentration(df: DataFrame, analysisDays: Int): DataFrame = {
    import df.sparkSession.implicits._
    // 窗口定义（按股票代码分区，按日期排序）
    val stockWindow = Window.partitionBy($"stock_code")
      .orderBy($"trade_date".asc)
      .rowsBetween(-analysisDays, 0)

    // 计算累积筹码分布
    val cumulativeDF = df
      .withColumn("cumulative_volume",
        sum($"daily_volume").over(stockWindow)
      )
      .groupBy($"stock_code", $"trade_date", $"price_level")
      .agg(max($"cumulative_volume").as("cumulative_volume"))

    // 集中度计算（前15%价格区间成交量占比）
    val concentrationDF = cumulativeDF
      .groupBy($"stock_code", $"trade_date")
      .agg(
        // 总成交量
        sum($"cumulative_volume").as("total_volume"),
        // 计算价格分位数
        expr("percentile_approx(price_level, 0.85)").as("price_85"),
        expr("percentile_approx(price_level, 0.15)").as("price_15")
      )
      .withColumn("concentration_ratio",
        when($"total_volume" > 0,
          ($"price_85" - $"price_15") / ($"price_85" + $"price_15"))
          .otherwise(lit(null))
      )
      .select($"stock_code", $"trade_date", $"concentration_ratio")

    // 结果格式化
    concentrationDF
      .withColumn("concentration_ratio", round($"concentration_ratio", 4))
      .orderBy($"stock_code", $"trade_date")
  }
}