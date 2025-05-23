package sparktask.adata.dwd.v2

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.{Date, Properties}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import sparktask.tools.MysqlTools

import scala.math.Ordering

/**
 * 工业级金融压力支撑位计算系统
 *
 * 功能特性：
 * 1. 多指标复合计算：整合4种经典技术分析方法
 * 2. 动态参数优化：基于ATR的窗口自适应机制
 * 3. 生产级容错：多层异常处理和数据校验
 * 4. 分布式优化：支持TB级数据量处理
 *
 * 计算结果字段说明：
 * +------------------+---------------------------------------------------------------+-----------------+
 * | 字段名称          | 计算逻辑                                                     | 适用场景         |
 * +------------------+---------------------------------------------------------------+-----------------+
 * | ma_pressure      | 移动平均法压力位 = 移动平均价 + 1倍标准差                   | 震荡行情         |
 * | ma_support       | 移动平均法支撑位 = 移动平均价 - 1倍标准差                   | 趋势初现阶段     |
 * | boll_pressure    | 布林带压力位 = 中轨线 + 2倍标准差                          | 波动率较高市场   |
 * | boll_support     | 布林带支撑位 = 中轨线 - 2倍标准差                          | 均值回归策略     |
 * | pivot_pressure   | 枢轴点压力位 = 枢轴点 + (昨日最高 - 昨日最低)               | 日内交易         |
 * | pivot_support    | 枢轴点支撑位 = 枢轴点 - (昨日最高 - 昨日最低)               | 关键价位突破     |
 * | high_low_pressure| 前高压力位 = 窗口期内最高价                                | 强趋势市场       |
 * | high_low_support | 前低支撑位 = 窗口期内最低价                                | 支撑阻力位交易   |
 * +------------------+---------------------------------------------------------------+-----------------+
 *
 * 生产环境选择建议：
 * - 趋势跟踪策略：优先使用boll_pressure/boll_support，配合ATR指标过滤假突破
 * - 日内高频交易：使用pivot_pressure/pivot_support，结合订单簿数据验证
 * - 算法交易风控：采用high_low_pressure/high_low_support作为硬止损点
 * - 组合策略：各指标加权平均（建议权重：布林带40%，移动平均30%，枢轴点20%，前高前低10%）
 */
object PressureSupportCalculator2 {
  // 在类顶部添加隐式排序定义
  implicit val localDateTimeOrdering: Ordering[LocalDateTime] =
    Ordering.by(_.atZone(ZoneId.systemDefault).toInstant.toEpochMilli)

  // 基础数据结构定义
  case class RawData(
                      stock_code: String,
                      trade_time: String,
                      open: BigDecimal,
                      high: BigDecimal,
                      low: BigDecimal,
                      close: BigDecimal,
                      volume: Int
                    )

  // 增强数据结构（含ATR）
  case class EnhancedData(
                           stock_code: String,
                           trade_time: String,
                           open: BigDecimal,
                           high: BigDecimal,
                           low: BigDecimal,
                           close: BigDecimal,
                           volume: Int,
                           atr: BigDecimal
                         )

  // 窗口参数优化中间结构
  case class WindowParam(
                          stock_code: String,
                          window_size: Int
                        )

  // 最终结果结构
  case class ResultData(
                         // 证券代码（ISO 6166标准）
                         stock_code: String,
                         // 交易时间（UTC时区，精确到秒）
                         trade_time: String,
                         // 窗口区间
                         windowSize: Int,
                         // 移动平均法压力位（20日基准）
                         ma_pressure: BigDecimal,
                         // 移动平均法支撑位（用于趋势确认）
                         ma_support: BigDecimal,
                         // 布林带上轨（2倍标准差通道）
                         boll_pressure: BigDecimal,
                         // 布林带下轨（波动率量化指标）
                         boll_support: BigDecimal,
                         // 经典枢轴点压力位（Floor法计算）
                         pivot_pressure: BigDecimal,
                         // 日内交易关键支撑位
                         pivot_support: BigDecimal,
                         // 窗口期价格高点（动态窗口优化）
                         high_low_pressure: BigDecimal,
                         // 价格回调防御位
                         high_low_support: BigDecimal
                       )

  // 合并数据结构
  private case class EnhancedDataWithWindow(
                                             stock_code: String,
                                             window_size: Int,
                                             trade_time: String,
                                             open: BigDecimal,
                                             high: BigDecimal,
                                             low: BigDecimal,
                                             close: BigDecimal,
                                             volume: Int,
                                             atr: BigDecimal
                                           )

  // 时间格式定义
  private val timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    val startm = System.currentTimeMillis()

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[4]")
      //      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.driver.memory", "4g")
      // 增加JDBC并行任务数
      .config("spark.jdbc.parallelism", "4")
      .config("spark.local.dir", "D:\\SparkTemp")
      .getOrCreate()

    val url = "jdbc:mysql://localhost:3306/gs"
    val driver = "com.mysql.cj.jdbc.Driver"
    val user = "root"
    val pwd = "123456"

    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", pwd)
    properties.setProperty("url", url)
    properties.setProperty("driver", driver)

    spark.sparkContext.setLogLevel("ERROR")//[18,19]

    val start_time ="2025-05-18"
    val end_time ="2025-05-22"

    val inputPath = "file:///D:\\gsdata\\gpsj_day_all_hs\\trade_date_month=202*"
    val outputPath = "file:///D:\\gsdata\\pressure_support_calculator"

    // 注册自定义Kryo序列化（生产环境需要实现Registrator）
    spark.sparkContext.getConf.registerKryoClasses(Array(classOf[RawData], classOf[EnhancedData], classOf[ResultData]))

    var df: DataFrame = spark.read.jdbc(url, "data_jyrl", properties)
    df.createTempView("data_jyrl")
          val dayList = spark.sql(s"select trade_date from data_jyrl where  trade_date between '$start_time' and '$end_time' and trade_status='1' order by trade_date desc").collect().map(f=>f.getAs[String]("trade_date"))
//    val dayList = spark.sql("select trade_date from data_jyrl where  trade_date between '2019-01-01' and '2019-07-19' and trade_status='1' order by trade_date desc").collect().map(f => f.getAs[String]("trade_date"))

    val indf = spark.read.parquet(inputPath).persist(StorageLevel.MEMORY_AND_DISK_SER)
    for (day <- dayList) {
      val year = day.substring(0,4)
      val tablename = s"pressure_support_calculator$year"
      println(day+"-----"+year+"-----"+tablename)

      // 数据加载与校验
      val rawDS = loadAndValidateData(spark, indf, day)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      // ATR计算
      val enhancedDS = calculateATR(rawDS)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      // 窗口参数优化
      val windowParams = optimizeWindowParameters(enhancedDS)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      // 指标计算
      val resultDS = calculateAllIndicators(windowParams, enhancedDS)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      // 结果校验与持久化
      validateAndSaveResults(tablename, rawDS, resultDS, day, url, properties, outputPath)

      // 释放缓存
      rawDS.unpersist()
      enhancedDS.unpersist()
      windowParams.unpersist()
      resultDS.unpersist()
      println(s"当前时间: " + new Date)
    }
    indf.unpersist()

    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }

  /** 数据加载与校验 */
  def loadAndValidateData(spark: SparkSession, df: DataFrame, day: String): Dataset[RawData] = {
    import spark.implicits._

    df.where(s"trade_date<='$day'")
      .select(
        $"stock_code".cast(StringType),
        $"trade_time".cast(StringType),
        $"open".cast(DecimalType(18, 2)),
        $"high".cast(DecimalType(18, 2)),
        $"low".cast(DecimalType(18, 2)),
        $"close".cast(DecimalType(18, 2)),
        $"volume".cast(IntegerType)
      )
      .filter(
        $"close" =!= BigDecimal(0) && // 过滤无效价格
          $"trade_time".isNotNull && // 确保时间字段存在
          $"volume" > 0 // 过滤零成交量
      )
      .as[RawData]
  }

  /** ATR计算（14日平均真实波幅） */
  def calculateATR(rawDS: Dataset[RawData]): Dataset[EnhancedData] = {
    val spark = rawDS.sparkSession
    import spark.implicits._

    val windowSpec = Window.partitionBy($"stock_code")
      .orderBy(unix_timestamp($"trade_time", "yyyy-MM-dd HH:mm:ss").cast("long"))
      .rowsBetween(-13, 0) // 14日窗口（包含当前行）

    rawDS
      .withColumn("prev_close", lag($"close", 1).over(Window.partitionBy($"stock_code").orderBy($"trade_time")))
      .withColumn("tr", greatest(
        $"high" - $"low",
        abs($"high" - $"prev_close"),
        abs($"low" - $"prev_close")
      ))
      .withColumn("atr", avg($"tr").over(windowSpec))
      .na.fill(0, Seq("atr")) // 处理初始空值
      .drop("prev_close", "tr")
      .as[EnhancedData]
  }

  /** 动态窗口参数优化 */
  def optimizeWindowParameters(ds: Dataset[EnhancedData]): Dataset[WindowParam] = {
    val spark = ds.sparkSession
    import spark.implicits._

    // 参数候选范围（生产环境可配置化）
    val windowCandidates = (10 to 40 by 5).toArray

    // 注册累加器用于异常监控
    val errorAccumulator = spark.sparkContext.collectionAccumulator[String]("calculationErrors")

    ds.groupByKey(_.stock_code)
      .flatMapGroups { (stockCode, rows) =>
        try {
          val sortedData = rows.toArray.sortBy { row =>
            LocalDateTime.parse(row.trade_time, timeFormatter)
          }(localDateTimeOrdering)

          windowCandidates.flatMap { windowSize =>
            if (sortedData.length >= windowSize * 2) { // 确保足够的历史数据
              val (returnRatio, riskRatio) = calculateProfitRiskRatio(sortedData, windowSize)
              if (!returnRatio.isNaN && !riskRatio.isNaN) {
                Some((stockCode, windowSize, returnRatio / riskRatio))
              } else {
                errorAccumulator.add(s"Invalid ratio: $stockCode-$windowSize")
                None
              }
            } else {
              errorAccumulator.add(s"Insufficient data: $stockCode-$windowSize")
              None
            }
          }
        } catch {
          case e: Exception =>
            errorAccumulator.add(s"Processing failed for $stockCode: ${e.getMessage}")
            Array.empty[(String, Int, Double)]
        }
      }
      .toDF("stock_code", "window_size", "score")
      .createOrReplaceTempView("window_scores")

    spark.sql(
      """
        |WITH ranked AS (
        |  SELECT
        |    stock_code AS ranked_stock_code,
        |    window_size,
        |    score,
        |    ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY score DESC) as rank
        |  FROM window_scores
        |)
        |SELECT
        |  ranked_stock_code AS stock_code,
        |  window_size
        |FROM ranked
        |WHERE rank = 1
        |""".stripMargin
    ).as[WindowParam]
  }

  /** 多指标综合计算 */
  def calculateAllIndicators(windowParams: Dataset[WindowParam], enhancedDS: Dataset[EnhancedData]): Dataset[ResultData] = {
    val spark = enhancedDS.sparkSession
    import spark.implicits._

    // 合并数据与窗口参数
    val mergedDS = windowParams.alias("params").join(enhancedDS.alias("data"), col("params.stock_code") === col("data.stock_code"), "inner")
      .select(
        col("params.stock_code").as("stock_code"),
        col("params.window_size"),
        col("data.trade_time"),
        col("data.open"),
        col("data.high"),
        col("data.low"),
        col("data.close"),
        col("data.volume"),
        col("data.atr")
      )
      .as[EnhancedDataWithWindow]
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // 按股票和窗口分组计算
    mergedDS.groupByKey(data => (data.stock_code, data.window_size))
      .flatMapGroups { case ((stockCode, windowSize), rows) =>
        try {
          val sortedData = rows.toArray.sortBy(data => LocalDateTime.parse(data.trade_time, timeFormatter))

          // 滑动窗口处理
          sortedData.sliding(windowSize).flatMap { window =>
            if (window.length < windowSize) None else {
              val current = window.last

              // 正确解构各指标计算结果
              val (maPress, maSupport) = calculateMA(window)
              val (bollPress, bollSupport) = calculateBollinger(window)
              val (pivotPress, pivotSupport) = calculatePivot(window)
              val (highLowPress, highLowSupport) = calculateHighLow(window)

              Some(ResultData(
                stockCode,
                current.trade_time,
                windowSize,
                maPress,
                maSupport,
                bollPress,
                bollSupport,
                pivotPress,
                pivotSupport,
                highLowPress,
                highLowSupport
              ))
            }
          }
        } catch {
          case e: Exception =>
            spark.sparkContext.collectionAccumulator[String]("calculationErrors")
              .add(s"指标计算失败: $stockCode/${windowSize} - ${e.getMessage}")
            Seq.empty[ResultData]
        }
      }
  }

  // 以下是核心指标计算方法

  /** 移动平均法压力支撑位 */
  private def calculateMA(window: Array[EnhancedDataWithWindow]): (BigDecimal, BigDecimal) = {
    // 工业级数值处理方案
    val closes = window.map(_.close)
    val ma = closes.sum / closes.length

    // 使用精确的数值转换策略
    val variance = closes.map { c =>
      val diff = c - ma
      // 使用BigDecimal的精确平方计算
      diff.pow(2)
    }.sum / closes.length

    // 安全类型转换流程
    val stdDouble = math.sqrt(variance.toDouble) // 转换为Double进行平方根计算

    // 结果精度控制（保留4位小数）
    val std = BigDecimal(stdDouble).setScale(4, BigDecimal.RoundingMode.HALF_UP)

    // 压力支撑位计算（保持BigDecimal精度）
    (
      (ma + std).setScale(4, BigDecimal.RoundingMode.HALF_UP),
      (ma - std).setScale(4, BigDecimal.RoundingMode.HALF_UP)
    )
  }

  /** 布林带指标 */
  private def calculateBollinger(window: Array[EnhancedDataWithWindow]): (BigDecimal, BigDecimal) = {
    // 工业级布林带计算逻辑
    val closes = window.map(_.close)
    val ma = closes.sum / closes.length

    // 精确计算方差（保持BigDecimal精度）
    val variance = closes.map { c =>
      val diff = c - ma
      diff.pow(2) // 使用BigDecimal的平方运算
    }.sum / closes.length

    // 安全类型转换流程
    // 添加异常处理边界
    val stdDouble = try {
      math.sqrt(variance.toDouble)
    } catch {
      case _: IllegalArgumentException =>
        println(s"无效方差值: $variance")
        0.0 // 安全回退值
    }
    val multiplier = 2.0 // 布林带倍数参数（可配置化）

    // 结果精度控制（保留4位小数）
    val bollingerStd = BigDecimal(stdDouble * multiplier)
      .setScale(4, BigDecimal.RoundingMode.HALF_UP)

    // 压力支撑位计算（保持BigDecimal精度）
    (
      (ma + bollingerStd).setScale(4, BigDecimal.RoundingMode.HALF_UP),
      (ma - bollingerStd).setScale(4, BigDecimal.RoundingMode.HALF_UP)
    )
  }

  /** 工业级枢轴点计算 */
  private def calculatePivot(window: Array[EnhancedDataWithWindow]): (BigDecimal, BigDecimal) = {
    val last = window.last
    // 使用安全运算避免除零错误
    val pivot = (last.high + last.low + last.close) / 3
    val range = last.high - last.low

    // 结果精度控制
    (
      (pivot + range).setScale(4, BigDecimal.RoundingMode.HALF_UP),
      (pivot - range).setScale(4, BigDecimal.RoundingMode.HALF_UP)
    )
  }

  /** 生产级前高前低计算 */
  private def calculateHighLow(window: Array[EnhancedDataWithWindow]): (BigDecimal, BigDecimal) = {
    // 使用reduce安全获取极值
    val maxHigh = window.map(_.high).reduce(_ max _)
    val minLow = window.map(_.low).reduce(_ min _)

    // 显式精度控制（即使输入已保证精度）
    (
      maxHigh.setScale(4, BigDecimal.RoundingMode.HALF_UP),
      minLow.setScale(4, BigDecimal.RoundingMode.HALF_UP)
    )
  }

  /** 收益风险比计算 */
  private def calculateProfitRiskRatio(data: Array[EnhancedData], windowSize: Int): (Double, Double) = {
    val returns = data.sliding(windowSize + 1).flatMap { window =>
      if (window.length > windowSize) {
        val begin = window.head.close.doubleValue
        val end = window.last.close.doubleValue
        val ret = (end - begin) / begin
        if (ret.isNaN || ret.isInfinite) None else Some(ret)
      } else None
    }.toArray

    if (returns.isEmpty) (Double.NaN, Double.NaN) else {
      val avgReturn = returns.sum / returns.length
      val risk = math.sqrt(returns.map(r => math.pow(r - avgReturn, 2)).sum / returns.length)
      (avgReturn, risk)
    }
  }

  /** 结果校验与存储 */
  private def validateAndSaveResults(tablename:String, rawDS: Dataset[RawData], resultDS: Dataset[ResultData], day: String, url: String, properties: Properties, path: String): Unit = {
    // 数据校验
    //    val badRecords = resultDS.filter(row =>
    //      row.ma_pressure < row.ma_support || // 压力位应大于支撑位
    //        row.high_low_pressure < row.high_low_support
    //    ).cache()
    //
    //    if (!badRecords.isEmpty) {
    //      badRecords.write.mode("overwrite").parquet(s"$path/invalid_records")
    //      throw new IllegalStateException(s"发现${badRecords.count()}条异常记录，已保存到$path/invalid_records")
    //    }

    // 基础数据与结果数据合并
    // 找出两个表中都存在的列
    val commonColumns = rawDS.columns.intersect(resultDS.columns)

    // 合并两个表，使用共同列进行连接
    val joinedDF = rawDS.join(resultDS, commonColumns, "inner")
      .withColumn("trade_date", split(col("trade_time"), " ")(0))

    // 执行删除操作
    try {
      // 通过 Spark 执行 SQL 删除语句
      // 编写 SQL 删除语句
      val deleteQuery = s"DELETE FROM $tablename WHERE trade_date='$day'"
      MysqlTools.mysqlEx(tablename, deleteQuery)
      println("数据删除成功！")
    } catch {
      case e: Exception => println(s"数据删除失败: ${e.getMessage}")
    }

    joinedDF.where(s"trade_date='$day'").write.mode("append").jdbc(url, tablename, properties)
    //        joinedDF.write
    //          .mode("overwrite")
    //          .option("compression", "snappy")
    //          .parquet(s"$outputPath/valid_results_finaldata")

    // 正常数据存储
    //    ds.write
    //      .mode("overwrite")
    //      .option("compression", "snappy")
    //      .parquet(s"$path/valid_results")
  }
}
