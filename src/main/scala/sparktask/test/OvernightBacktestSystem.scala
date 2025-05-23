//package spark.test
//import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}
//import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types._
//import java.time.LocalDateTime
//import java.time.format.DateTimeFormatter
//
//
//object OvernightBacktestSystem {
//
//  // 输入数据结构
//  case class RawData(stock_code: String,
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
//                           trade_time: String,
//                           close: BigDecimal,
//                           volume: Int,
//                           ma_pressure: BigDecimal,
//                           ma_support: BigDecimal,
//                           boll_pressure: BigDecimal,
//                           boll_support: BigDecimal,
//                           pivot_pressure: BigDecimal,
//                           pivot_support: BigDecimal,
//                           high_low_pressure: BigDecimal,
//                           high_low_support: BigDecimal
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
//                             pressure_types: String
//                           )
//
//  def main(args: Array[String]): Unit = {
//    // 本地Spark配置
//    val spark = SparkSession.builder()
//      .appName("LocalOvernightBacktest")
//      .master("local[*]")  // 使用4个本地核心
//      .config("spark.driver.memory", "8g")
//      .config("spark.executor.memory", "2g")
//      .config("spark.sql.shuffle.partitions", "10")
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .getOrCreate()
//
//    import spark.implicits._
//
//    // 1. 数据加载
//    val rawDF = spark.read.parquet("path/to/local/parquet")
//      .select(
//        col("stock_code").cast(StringType),
//        col("trade_time").cast(StringType),
//        col("open").cast(DecimalType(18, 4)),
//        col("high").cast(DecimalType(18, 4)),
//        col("low").cast(DecimalType(18, 4)),
//        col("close").cast(DecimalType(18, 4)),
//        col("volume").cast(IntegerType)
//      ).as[RawData]
//
//    // 2. 技术指标计算
//    val enhancedDS = calculateIndicators(rawDF)
//
//    // 3. 风险标记
//    val riskMarkedDS = markRisks(enhancedDS)
//
//    // 4. 回测执行
//    val backtestResults = runBacktest(riskMarkedDS)
//
//    // 5. 结果展示
//    backtestResults.show(20, truncate = false)
//
//    spark.stop()
//  }
//
//  /** 指标计算（工业级实现） */
//  def calculateIndicators(rawDS: Dataset[RawData]): Dataset[EnhancedData] = {
//    val windowSpec = Window.partitionBy("stock_code")
//      .orderBy(col("trade_time").cast("timestamp"))
//      .rowsBetween(-20, 0)
//
//    rawDS.map { row =>
//      // 移动平均法计算
//      val closes = Array(row.close)
//      val ma = closes.sum / closes.length
//      val maStd = closes.map(c => (c - ma).pow(2)).sum / closes.length
//
//      // 布林带计算
//      val bollStd = maStd * 2
//
//      // 枢轴点计算
//      val pivot = (row.high + row.low + row.close) / 3
//      val pivotRange = row.high - row.low
//
//      EnhancedData(
//        row.stock_code,
//        row.trade_time,
//        row.close,
//        row.volume,
//        (ma + maStd).setScale(2, BigDecimal.RoundingMode.HALF_UP),
//        (ma - maStd).setScale(2, BigDecimal.RoundingMode.HALF_UP),
//        (ma + bollStd).setScale(2, BigDecimal.RoundingMode.HALF_UP),
//        (ma - bollStd).setScale(2, BigDecimal.RoundingMode.HALF_UP),
//        (pivot + pivotRange).setScale(2, BigDecimal.RoundingMode.HALF_UP),
//        (pivot - pivotRange).setScale(2, BigDecimal.RoundingMode.HALF_UP),
//        row.high.setScale(2, BigDecimal.RoundingMode.HALF_UP),
//        row.low.setScale(2, BigDecimal.RoundingMode.HALF_UP)
//      )
//    }
//  }
//
//  /** 工业级风险标记系统 */
//  def markRisks(ds: Dataset[EnhancedData]): Dataset[EnhancedData] = {
//    ds.withColumn("risk_flags", concat_ws(",",
//      when(col("close") < col("ma_support") * 0.98, "MA支撑破位").otherwise(""),
//      when(col("volume") < 500000, "流动性不足").otherwise(""),
//      when(col("close") > col("boll_pressure") * 1.02, "超买风险").otherwise(""),
//      when(col("high_low_pressure") - col("high_low_support") < 0.05, "波动不足").otherwise("")
//    )).as[EnhancedData]
//  }
//
//  /** 生产级回测引擎 */
//  def runBacktest(ds: Dataset[EnhancedData]): Dataset[BacktestResult] = {
//    ds.groupByKey(_.stock_code)
//      .flatMapGroups { (code, rows) =>
//        val sorted = rows.toArray.sortBy(_.trade_time)
//        sorted.sliding(2).flatMap {
//          case Array(prev, current) =>
//            val entry = prev.close
//            val exit = current.close
//            val holdingDays = daysBetween(prev.trade_time, current.trade_time)
//
//            val riskFlags = current.risk_flags.split(",").filter(_.nonEmpty)
//
//            Some(BacktestResult(
//              code,
//              current.trade_time,
//              entry.setScale(2, BigDecimal.RoundingMode.HALF_UP),
//              exit.setScale(2, BigDecimal.RoundingMode.HALF_UP),
//              holdingDays,
//              ((exit - entry) / entry * 100).setScale(2, BigDecimal.RoundingMode.HALF_UP),
//              if (riskFlags.isEmpty) "无" else riskFlags.mkString("|"),
//              pressureTypeDetection(current)
//            ))
//          case _ => None
//        }
//      }
//  }
//
//  /** 压力位类型检测（工业级逻辑） */
//  private def pressureTypeDetection(data: EnhancedData): String = {
//    val conditions = Seq(
//      (data.close > data.boll_pressure * 0.99, "布林带上轨"),
//      (data.close > data.pivot_pressure * 0.995, "枢轴压力"),
//      (data.close > data.high_low_pressure * 0.99, "前高压制")
//    ).filter(_._1).map(_._2)
//
//    if (conditions.isEmpty) "无压力" else conditions.mkString("+")
//  }
//
//  /** 日期差值计算 */
//  private def daysBetween(start: String, end: String): Int = {
//    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
//    val startDate = LocalDateTime.parse(start, formatter)
//    val endDate = LocalDateTime.parse(end, formatter)
//    Duration.between(startDate, endDate).toDays.toInt.abs
//  }
//}
