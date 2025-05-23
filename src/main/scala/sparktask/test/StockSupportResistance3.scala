package sparktask.test

/*
 * 股票支撑压力位计算系统
 * 版本：1.3.1
 * 修改说明：
 * 1. 明确各阶段数据结构字段变化
 * 2. 验证ATR字段传递流程
 * 3. 增强代码注释说明
 */

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import java.sql.Date

import org.apache.spark.SparkConf

object StockSupportResistance3 {

  // region 数据结构定义 ====================================================

  /** 原始输入数据（从Parquet读取） */
  case class RawStockData(
                           symbol: String,
                           date: Date,
                           open: Double,
                           high: Double,
                           low: Double,
                           close: Double,
                           volume: Int
                         )

  /** 预处理中间数据（平滑处理后）*/
  case class SmoothedData(
                           symbol: String,
                           date: Date,
                           smoothed_high: Double,
                           smoothed_low: Double,
                           smoothed_close: Double
                         )

  /** ATR处理后的完整数据 */
  case class ProcessedData(
                            symbol: String,
                            date: Date,
                            smoothed_high: Double,
                            smoothed_low: Double,
                            smoothed_close: Double,
                            atr: Double  // 此字段在calculateATR阶段添加
                          )

  /** 优化后的窗口参数 */
  case class OptimizedWindow(symbol: String, window_size: Int)

  /** 最终指标结果 */
  case class IndicatorResult(
                              symbol: String,
                              date: Date,
                              pivot_support: Double,
                              pivot_resistance: Double,
                              bollinger_support: Double,
                              bollinger_resistance: Double,
                              prev_high_resistance: Double,
                              prev_low_support: Double,
                              ma_support: Double,
                              optimized_window: Int
                            )

  // endregion

  // region 主程序入口 ======================================================
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.stock_codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "200")
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

    import spark.implicits._

    try {
      // 阶段1：数据预处理（不含ATR）
      val smoothedData = loadAndCleanData(spark)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      smoothedData.show()

      // 阶段2：ATR计算（添加ATR字段）
      val processedData = calculateATR(smoothedData)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      processedData.show()

      // 阶段3：参数优化（使用含ATR的数据）
      val optimizedWindows = optimizeParameters(processedData)

      optimizedWindows.show()
      // 阶段4：指标计算
      val result = calculateIndicators(processedData, optimizedWindows)

      result.show()

      // 结果输出
//      optimizedWindows
//        .write.mode("overwrite")
//        .parquet("file:///D:\\gsdata\\test_0209")
//      result
//        .write.mode("overwrite").partitionBy("symbol")
//        .parquet("file:///D:\\gsdata\\test_0209_1")

    } finally {
      spark.stop()
    }
  }
  // endregion

  // region 数据预处理模块 ==================================================

  /**
   * 数据加载与清洗流程
   * 输出：SmoothedData（不含ATR）
   */
  def loadAndCleanData(spark: SparkSession): Dataset[SmoothedData] = {
    import spark.implicits._

    val df1 = spark.read.parquet("file:///D:\\gsdata\\gpsj_day_all_hs")
      .select( col("stock_code").as("symbol"), to_date(col("trade_time"), "yyyy-MM-dd").as("date"),  col("open").cast("double"), col("high").cast("double"), col("low").cast("double"), col("close").cast("double"), col("volume"))
      .as[RawStockData]
      .filter(r =>
        r.high >= r.low &&
          r.close <= r.high &&
          r.low > 0 &&
          r.volume > 0
      )

      .transform(smoothData)
      df1.show()
      df1.transform(removeOutliers)
  }

  /**
   * 数据平滑处理（不包含ATR字段）
   */
  private def smoothData(data: Dataset[RawStockData]): Dataset[SmoothedData] = {
    // 在每个需要Dataset操作的方法内部显式导入
    import data.sparkSession.implicits._
    val windowSpec = Window.partitionBy($"symbol").orderBy($"date").rowsBetween(-4, 0)

    data.withColumn("smoothed_high", avg($"high").over(windowSpec))
      .withColumn("smoothed_low", avg($"low").over(windowSpec))
      .withColumn("smoothed_close", avg($"close").over(windowSpec))
      .select($"symbol", $"date", $"smoothed_high", $"smoothed_low", $"smoothed_close")
      .as[SmoothedData]
  }

  /**
   * 异常值处理（仍为SmoothedData类型）
   */
  private def removeOutliers(data: Dataset[SmoothedData]): Dataset[SmoothedData] = {
    // 在每个需要Dataset操作的方法内部显式导入
    import data.sparkSession.implicits._
    val volatilityWindow = Window
      .partitionBy($"symbol")
      .orderBy($"date")
      .rowsBetween(-19, 0)

    data
      .withColumn("median", expr("percentile_approx(smoothed_close, 0.5)").over(volatilityWindow))
      .withColumn("mad", avg(abs($"smoothed_close" - $"median")).over(volatilityWindow))
      .filter(abs($"smoothed_close" - $"median") <  $"mad" * 3)
      .drop("median", "mad")
      .as[SmoothedData]
  }

  // endregion

  // region ATR计算模块 =====================================================

  /**
   * 计算ATR并升级为ProcessedData
   */
  def calculateATR(data: Dataset[SmoothedData]): Dataset[ProcessedData] = {
    // 在每个需要Dataset操作的方法内部显式导入
    import data.sparkSession.implicits._
    val windowSpec = Window
      .partitionBy($"symbol")
      .orderBy($"date")

    data
      .withColumn("prev_close", lag($"smoothed_close", 1).over(windowSpec))
      .withColumn("tr", greatest(
        $"smoothed_high" - $"smoothed_low",
        abs($"smoothed_high" - $"prev_close"),
        abs($"smoothed_low" - $"prev_close")
      ))
      .withColumn("atr", avg($"tr").over(windowSpec.rowsBetween(-13, 0)))
      .select(
        $"symbol",
        $"date",
        $"smoothed_high",
        $"smoothed_low",
        $"smoothed_close",
        $"atr"
      )
      .as[ProcessedData]  // 转换为包含ATR的ProcessedData
  }

  // endregion

  // region 参数优化模块 ====================================================

  /**
   * 动态窗口优化（使用包含ATR的ProcessedData）
   */
  def optimizeParameters(data: Dataset[ProcessedData]): Dataset[OptimizedWindow] = {
    // 在每个需要Dataset操作的方法内部显式导入
    import data.sparkSession.implicits._
    data.groupByKey(_.symbol) // 按股票分组优化
      .flatMapGroups { case (symbol, records) =>
        val sortedData = records.toSeq.sortBy(_.date.getTime) // 时间排序
        val candidateWindows = Seq(10, 14, 20, 30, 50) // 候选窗口参数

        // 计算每个窗口的夏普比率
        val validCandidates = candidateWindows.flatMap { window =>
          if (sortedData.size >= window) {
            val sharpe = calculateSharpeRatio(sortedData, window)
            if (sharpe > 0) Some((symbol, window, sharpe)) else None
          } else None
        }

        // 选择夏普比率最高的窗口
        validCandidates match {
          case Nil => Seq.empty // 无有效候选时跳过
          case nonEmpty =>
            val best = nonEmpty.maxBy(_._3)
            Seq(OptimizedWindow(best._1, best._2))
        }
      }
  }

  /**
   * 窗口评估（使用ATR数据进行波动率分析）
   */
  private def evaluateWindow(
                              data: Seq[ProcessedData],  // 包含ATR数据
                              window: Int,
                              symbol: String
                            ): Option[(String, Int, Double)] = {
    if (data.size < window) None else {
      // 示例：结合ATR计算波动率调整后的夏普比率
      val baseSharpe = calculateSharpeRatio(data, window)
      val volatility = data.takeRight(window).map(_.atr).sum / window
      val adjustedSharpe = baseSharpe / volatility

      if (adjustedSharpe > 0) Some((symbol, window, adjustedSharpe)) else None
    }
  }

  /**
   * 夏普比率计算（使用平滑后的收盘价）
   */
  private def calculateSharpeRatio(data: Seq[ProcessedData], window: Int): Double = {
    val returns = data.sliding(window).collect {
      case chunk if chunk.size >= window =>
        (chunk.last.smoothed_close - chunk.head.smoothed_close) / chunk.head.smoothed_close
    }

    if (returns.isEmpty) 0.0 else {
      val mean = returns.sum / returns.size
      val std = math.sqrt(returns.map(r => math.pow(r - mean, 2)).sum / returns.size)
      if (std != 0) mean / std else 0.0
    }
  }

  // endregion

  // region 指标计算模块 ====================================================

  /**
   * 综合指标计算（使用包含ATR的完整数据）
   */
  def calculateIndicators(
                           data: Dataset[ProcessedData],
                           windows: Dataset[OptimizedWindow]
                         ): Dataset[IndicatorResult] = {
    import data.sparkSession.implicits._

    data.joinWith(windows, data("symbol") === windows("symbol"))
      .flatMap { case (stock, window) =>
        computeIndicatorsForStock(stock, window, data)
      }(Encoders.product[IndicatorResult])
  }

  /**
   * 单个股票指标计算
   */
  private def computeIndicatorsForStock(
                                         stock: ProcessedData,
                                         window: OptimizedWindow,
                                         dataset: Dataset[ProcessedData]
                                       ): Option[IndicatorResult] = {
    // 在每个需要Dataset操作的方法内部显式导入
    import dataset.sparkSession.implicits._
    val history = dataset.filter(_.symbol == stock.symbol)
      .sort($"date")
      .collect()
      .toSeq

    val windowData = history.takeRight(window.window_size)

    if (windowData.size >= window.window_size) {
      Some(IndicatorResult(
        symbol = stock.symbol,
        date = stock.date,
        pivot_support = calculatePivotSupport(windowData),
        pivot_resistance = calculatePivotResistance(windowData),
        bollinger_support = calculateBollingerLower(windowData),
        bollinger_resistance = calculateBollingerUpper(windowData),
        prev_high_resistance = calculatePreviousHigh(windowData),
        prev_low_support = calculatePreviousLow(windowData),
        ma_support = calculateMovingAverage(windowData),
        optimized_window = window.window_size
      ))
    } else {
      None
    }
  }

  // region 指标计算方法 -----------------------------------------------------

  private def calculatePivotSupport(data: Seq[ProcessedData]): Double = {
    val high = data.map(_.smoothed_high).max
    val low = data.map(_.smoothed_low).min
    val close = data.last.smoothed_close
    2 * ((high + low + close) / 3) - high
  }

  private def calculatePivotResistance(data: Seq[ProcessedData]): Double = {
    val high = data.map(_.smoothed_high).max
    val low = data.map(_.smoothed_low).min
    val close = data.last.smoothed_close
    2 * ((high + low + close) / 3) - low
  }

  private def calculateBollingerLower(data: Seq[ProcessedData]): Double = {
    val closes = data.map(_.smoothed_close)
    val mean = closes.sum / closes.size
    val std = math.sqrt(closes.map(c => math.pow(c - mean, 2)).sum / closes.size)
    mean - 2 * std
  }

  private def calculateBollingerUpper(data: Seq[ProcessedData]): Double = {
    val closes = data.map(_.smoothed_close)
    val mean = closes.sum / closes.size
    val std = math.sqrt(closes.map(c => math.pow(c - mean, 2)).sum / closes.size)
    mean + 2 * std
  }

  private def calculatePreviousHigh(data: Seq[ProcessedData]): Double = {
    data.map(_.smoothed_high).max
  }

  private def calculatePreviousLow(data: Seq[ProcessedData]): Double = {
    data.map(_.smoothed_low).min
  }

  private def calculateMovingAverage(data: Seq[ProcessedData]): Double = {
    data.map(_.smoothed_close).sum / data.size
  }

  // endregion
  // endregion
}