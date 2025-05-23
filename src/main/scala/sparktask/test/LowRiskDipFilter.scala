package sparktask.test
package spark.test

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.math.BigDecimal
import org.apache.spark.sql.expressions.Window

/**
 * 工业级低吸模式风险过滤系统
 *
 * 系统设计要点：
 * 1. 采用四级风控过滤：支撑位验证、流动性检测、波动率控制、风险评分
 * 2. 支持动态精度控制，防止金融计算中的精度丢失
 * 3. 实现多维度支撑位聚合验证（MA/布林带/枢轴/前低）
 * 4. 内置工业级异常数据处理机制
 *
 * 数据流程：
 * 原始数据 -> 支撑位计算 -> 风险评分 -> 多级过滤 -> 结果输出
 */
object LowRiskDipFilter {

  // 原始数据结构定义（输入数据特征）
  case class StockData2(
                         stock_code: String,       // 证券代码（格式：SH600000/SZ000001）
                         trade_time: String,       // 交易时间（格式：yyyy-MM-dd HH:mm:ss）
                         close: BigDecimal,        // 收盘价（精度：小数点后2位）
                         ma_support: BigDecimal,   // MA支撑位（20日均线，精度：小数点后2位）
                         boll_support: BigDecimal, // 布林带下轨（20日标准差，精度：小数点后2位）
                         pivot_support: BigDecimal,// 枢轴点支撑（经典PP计算，精度：小数点后2位）
                         high_low_support: BigDecimal, // 前低支撑（近20日最低价，精度：小数点后2位）
                         volume: Int               // 当日成交量（单位：股）
                       )

  // 风险过滤结果结构
  case class FilteredResult(
                             stock_code: String,     // 证券代码
                             trade_time: String,     // 触发时间
                             close_price: BigDecimal,// 触发价格
                             nearest_support: BigDecimal, // 最强支撑位价格
                             support_distance: BigDecimal,// 价格与支撑位距离（百分比，如-2.5表示低于支撑位2.5%）
                             risk_score: Double,     // 综合风险评分（0-1，越小风险越低）
                             support_types: String   // 生效支撑类型（多个用|分隔）
                           )

  def main(args: Array[String]): Unit = {
    // Spark引擎配置（工业级参数优化）
    val spark = SparkSession.builder()
      .appName("LowRiskDipFilter")
      .master("local[4]")  // 本地模式使用4核
      .config("spark.driver.memory", "4g")  // 驱动内存设置
      .config("spark.sql.shuffle.partitions", "8")  // 优化shuffle性能
      .config("spark.sql.decimalOperations.allowPrecisionLoss", "false") // 严格精度模式
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // 数据输入输出路径（生产环境应配置为HDFS路径）
    val inputPath = "file:///D:\\gsdata\\pressure_support_calculator\\valid_results_finaldata"
    val outputPath = "file:///D:\\gsdata\\pressure_support_calculator\\valid_results_pressure_fx"

    // 阶段1：数据加载与预处理
    val stockDF = spark.read.parquet(inputPath)
      .select(
        // 显式类型转换确保数据一致性
        col("stock_code").cast(StringType),
        col("trade_time").cast(StringType),
        col("close").cast(DecimalType(18, 2)),       // 金融价格字段统一用Decimal(18,2)
        col("ma_support").cast(DecimalType(18, 2)),
        col("boll_support").cast(DecimalType(18, 2)),
        col("pivot_support").cast(DecimalType(18, 2)),
        col("high_low_support").cast(DecimalType(18, 2)),
        col("volume").cast(IntegerType)             // 成交量用整数存储
      ).as[StockData2]

    // 阶段2：核心指标计算
    val processedDF = stockDF
      // 计算最强支撑位（取各支撑位中的最大值）
      .withColumn("nearest_support", greatest(
        col("ma_support"),
        col("boll_support"),
        col("pivot_support"),
        col("high_low_support")
      ))
      // 计算价格与支撑位距离（百分比表示）
      .withColumn("support_distance",
        ((col("close") - col("nearest_support")) / col("nearest_support") * 100)
          .cast(DecimalType(5, 2)) // 限制范围(-999.99%~999.99%)
      )
      // 计算综合风险评分（数值越小风险越低）
      .withColumn("risk_score", calculateRiskScoreUDF(
        col("support_distance"),
        col("volume"),
        volatility(col("close"))  // 20日波动率
      ))

    // 阶段3：多级风险过滤
    val filteredDF = processedDF
      .filter(
        // 核心过滤条件（工业级参数）：
        col("close") < col("nearest_support") * 1.03 &&  // 价格在支撑位3%以内（缓冲区间）
          col("support_distance").between(-3.0, 1.0) &&    // 允许略微跌破（-3%）到接近支撑（+1%）
          col("volume") > 500000 &&                        // 日成交量>50万股（流动性门槛）
          col("risk_score") < 0.4 &&                       // 综合风险评分阈值
          validateSupportTypeCondition(col("ma_support"), col("boll_support"), col("pivot_support")) // 支撑位聚合验证
      )
      // 按风险评分升序排列（最安全标的在前）
      .sort(col("risk_score").asc)
      .select(
        col("stock_code"),
        col("trade_time"),
        col("close").as("close_price"),
        col("nearest_support"),
        col("support_distance"),
        col("risk_score"),
        // 识别生效的支撑类型（多个用|分隔）
        concat_ws("|",
          when(col("close") < col("ma_support") * 1.02, "MA支撑"),       // 价格在MA支撑2%范围内
          when(col("close") < col("boll_support") * 1.02, "布林带支撑"), // 价格在布林带支撑2%范围内
          when(col("close") < col("pivot_support") * 1.02, "枢轴支撑"),  // 价格在枢轴支撑2%范围内
          when(col("close") < col("high_low_support") * 1.02, "前低支撑")
        ).as("support_types")
      ).as[FilteredResult]

    // 阶段4：结果持久化与展示
    filteredDF.write.mode("overwrite").parquet(outputPath)
    println("===== 低吸候选标的（按风险评分排序）=====")
    filteredDF.show(20, truncate = false)  // 展示前20条结果

    spark.stop()
  }

  /**
   * 工业级风险评分计算器
   * 评分公式：风险评分 = (距离评分×50% + 波动率评分×30% + 流动性评分×20%)
   *
   * @param distance 价格与支撑位距离（百分比）
   * @param volume 当日成交量
   * @param volatility 20日价格波动率
   * @return 标准化风险评分[0,1]
   */
  private val calculateRiskScoreUDF = udf((distance: BigDecimal, volume: Int, volatility: Double) => {
      val distanceValue = distance.doubleValue
      // 距离评分：绝对值距离占允许最大距离的比例
      val distanceScore = math.abs(distanceValue) / 3.0  // 3%为最大允许距离

      // 流动性评分：成交量越大评分越低
      val volumeScore = 1 - math.min(volume / 2e6, 1.0)  // 200万股为满分阈值

      // 波动率评分：波动越大风险越高
      val volatilityScore = math.min(volatility / 0.3, 1.0) // 30%波动率为上限

      // 加权综合评分
      (distanceScore * 0.5 + volatilityScore * 0.3 + volumeScore * 0.2) / 1.0
    }
  )

  /**
   * 支撑位聚合验证（至少两个支撑位在1%范围内）
   * 防止单一支撑位的假突破
   *
   * @param ma MA支撑位
   * @param boll 布林带支撑位
   * @param pivot 枢轴支撑位
   * @return 是否满足聚合条件
   */
  private def validateSupportTypeCondition(ma: Column, boll: Column, pivot: Column): Column = {
    val threshold = lit(BigDecimal.valueOf(0.01)).cast(DecimalType(3,2)) // 1%阈值
    (
      (abs(ma - boll) < ma * threshold) || // MA与布林带支撑位接近
        (abs(ma - pivot) < ma * threshold) || // MA与枢轴支撑位接近
        (abs(boll - pivot) < boll * threshold) // 布林带与枢轴支撑位接近
      )
  }

  /**
   * 专业级波动率计算（20日滚动标准差）
   * 使用Double类型计算以提高性能，最后转换为百分比值
   *
   * @param price 价格序列
   * @return 20日波动率（标准差）
   */
  private def volatility(price: Column): Column = {
    val window = Window.partitionBy("stock_code")
      .orderBy(col("trade_time").cast("timestamp").cast("long")) // 时间戳排序
      .rowsBetween(-19, 0) // 20个交易日的窗口
    stddev(price.cast(DoubleType)).over(window) // 转为Double提升计算性能
  }
}