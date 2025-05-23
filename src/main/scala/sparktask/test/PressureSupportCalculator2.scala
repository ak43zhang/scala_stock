//package spark.test
//
//import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}
//import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types._
//import java.time.LocalDateTime
//import java.time.format.DateTimeFormatter
//
//object PressureSupportCalculator2 {
//
//  // 输入数据结构
//  case class RawData(
//                      stock_code: String,
//                      trade_time: String,
//                      open: BigDecimal,
//                      high: BigDecimal,
//                      low: BigDecimal,
//                      close: BigDecimal,
//                      volume: Int
//                    )
//
//  // 增强数据结构（含技术指标）
//  case class EnhancedData(
//                           stock_code: String,
//                           trade_date: String,
//                           close: BigDecimal,
//                           ma_pressure: BigDecimal,
//                           ma_support: BigDecimal,
//                           boll_pressure: BigDecimal,
//                           boll_support: BigDecimal,
//                           pivot_pressure: BigDecimal,
//                           pivot_support: BigDecimal,
//                           high_low_pressure: BigDecimal,
//                           high_low_support: BigDecimal,
//                           optimal_window: Int
//                         )
//
//  // 回测结果结构
//  case class BacktestResult(
//                             stock_code: String,
//                             trade_date: String,
//                             entry_price: BigDecimal,
//                             exit_price: BigDecimal,
//                             holding_days: Int,
//                             return_rate: BigDecimal,
//                             risk_flags: String,
//                             hit_pressure: Boolean
//                           )
//
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder()
//      .appName("LocalPressureSupportSystem")
//      .master("local[4]")
//      .config("spark.driver.memory", "4g")
//      .config("spark.sql.shuffle.partitions", "8")
//      .config("spark.sql.parquet.compression.codec", "snappy")
//      .getOrCreate()
//
//    import spark.implicits._
//
//    // 1. 数据加载
//    val rawDF = spark.read.parquet("path/to/parquet")
//      .select(
//        col("stock_code").cast(StringType),
//        col("trade_time").cast(StringType),
//        col("open").cast(DecimalType(18, 4)),
//        col("high").cast(DecimalType(18, 4)),
//        col("low").cast(DecimalType(18, 4)),
//        col("close").cast(DecimalType(18, 4)),
//        col("volume").cast(IntegerType)
//      ).as[RawData]
//      .withColumn("trade_date", to_date($"trade_time", "yyyy-MM-dd"))
//      .drop("trade_time")
//
//    // 2. 动态窗口优化
//    val windowParams = optimizeWindowParameters(rawDF)
//
//    // 3. 压力支撑位计算
//    val enhancedDS = calculatePressureSupport(rawDF, windowParams)
//
//    // 4. 风险标记
//    val riskMarkedDS = markRiskFlags(enhancedDS)
//
//    // 5. 回测执行
//    val backtestResults = runBacktest(riskMarkedDS)
//
//    // 6. 结果落地
//    backtestResults.write.mode("overwrite").parquet("output/backtest_results")
//
//    spark.stop()
//  }
//
//  /** 工业级窗口优化 */
//  def optimizeWindowParameters(rawDF: Dataset[RawData]): Dataset[(String, Int)] = {
//    val windowCandidates = (10 to 30 by 5).toArray
//
//    rawDF.groupByKey(_.stock_code)
//      .flatMapGroups { (code, rows) =>
//        val data = rows.toArray.sortBy(_.trade_date)
//        val returns = data.sliding(2).map {
//          case Array(prev, curr) => (curr.close - prev.close)/prev.close
//        }.toArray
//
//        windowCandidates.flatMap { window =>
//          if (data.length >= window * 2) {
//            val (avgReturn, risk) = calculateReturnRisk(returns, window)
//            if (!avgReturn.isNaN && !risk.isNaN) Some((code, window, avgReturn/risk))
//            else None
//          } else None
//        }
//      }.toDF("stock_code", "window_size", "score")
//      .groupBy("stock_code")
//      .agg(max(struct($"score", $"window_size")).as("best"))
//      .select($"stock_code", $"best.window_size".as("optimal_window"))
//      .as[(String, Int)]
//  }
//
//  /** 生产级压力支撑计算 */
//  def calculatePressureSupport(rawDF: Dataset[RawData], windowParams: Dataset[(String, Int)]): Dataset[EnhancedData] = {
//    val windowSpec = Window.partitionBy("stock_code").orderBy("trade_date")
//
//    rawDF.joinWith(windowParams.toDF(), $"stock_code" === $"value._1")
//      .map { case (data, params) =>
//        val windowSize = params.getAs[Int]("_2")
//
//        // 计算移动平均
//        val maWindow = Window.partitionBy("stock_code")
//          .orderBy("trade_date")
//          .rowsBetween(-windowSize, 0)
//
//        val ma = data.close + data.close.over(maWindow)
//        val maStd = stddev(data.close).over(maWindow)
//
//        // 计算布林带
//        val bollUpper = (ma + 2 * maStd).setScale(2, BigDecimal.RoundingMode.HALF_UP)
//        val bollLower = (ma - 2 * maStd).setScale(2, BigDecimal.RoundingMode.HALF_UP)
//
//        // 计算枢轴点
//        val pivot = (data.high + data.low + data.close)/3
//        val pivotRange = data.high - data.low
//
//        EnhancedData(
//          data.stock_code,
//          data.trade_date.toString,
//          data.close.setScale(2, BigDecimal.RoundingMode.HALF_UP),
//          (ma + maStd).setScale(2, BigDecimal.RoundingMode.HALF_UP),
//          (ma - maStd).setScale(2, BigDecimal.RoundingMode.HALF_UP),
//          bollUpper,
//          bollLower,
//          (pivot + pivotRange).setScale(2, BigDecimal.RoundingMode.HALF_UP),
//          (pivot - pivotRange).setScale(2, BigDecimal.RoundingMode.HALF_UP),
//          data.high.setScale(2, BigDecimal.RoundingMode.HALF_UP),
//          data.low.setScale(2, BigDecimal.RoundingMode.HALF_UP),
//          windowSize
//        )
//      }
//  }
//
//  /** 工业级风险标记系统 */
//  def markRiskFlags(ds: Dataset[EnhancedData]): Dataset[EnhancedData] = {
//    ds.withColumn("risk_flags",
//      when($"close" < $"ma_support" * 0.97, "MA支撑破位")
//        .when($"volume" < 500000, "流动性不足")
//        .when($"close" > $"boll_pressure" * 1.02, "超买风险")
//        .otherwise("")
//    )
//  }
//
//  /** 生产级回测引擎 */
//  def runBacktest(ds: Dataset[EnhancedData]): Dataset[BacktestResult] = {
//    ds.groupByKey(_.stock_code)
//      .flatMapGroups { (code, rows) =>
//        val sorted = rows.toArray.sortBy(_.trade_date)
//        sorted.sliding(2).flatMap {
//          case Array(prev, current) =>
//            val entry = prev.close
//            val exit = current.close
//            val days = daysBetween(prev.trade_date, current.trade_date)
//            val returnRate = ((exit - entry)/entry * 100).setScale(2, BigDecimal.RoundingMode.HALF_UP)
//
//            val hitPressure = current.close > current.boll_pressure ||
//              current.close > current.pivot_pressure
//
//            Some(BacktestResult(
//              code,
//              current.trade_date,
//              entry,
//              exit,
//              days,
//              returnRate,
//              current.risk_flags,
//              hitPressure
//            ))
//          case _ => None
//        }
//      }
//  }
//
//  /** 核心算法：收益风险计算 */
//  private def calculateReturnRisk(returns: Array[BigDecimal], window: Int): (Double, Double) = {
//    val slidingReturns = returns.sliding(window).map(_.sum).toArray
//    val avgReturn = slidingReturns.sum / slidingReturns.length
//    val risk = math.sqrt(slidingReturns.map(r => math.pow((r - avgReturn).toDouble, 2)).sum / slidingReturns.length)
//    (avgReturn.toDouble, risk)
//  }
//
//  /** 日期差值计算 */
//  private def daysBetween(start: String, end: String): Int = {
//    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
//    val startDate = LocalDateTime.parse(start, formatter)
//    val endDate = LocalDateTime.parse(end, formatter)
//    java.time.Duration.between(startDate, endDate).toDays.toInt
//  }
//}