package spark.bus

import java.math.BigDecimal
import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * 总线5
 *
 * 工业级低吸策略压力支撑位分析系统（注释增强版）
 *
 * 核心功能：
 * 1. 复合支撑/压力位计算：综合4种技术指标，确定安全边际和潜在空间
 * 2. 价格位置量化分析：通过标准化指标评估当前价格位置
 * 3. 动态支撑/压力类型识别：识别当前生效的技术指标
 * 4. 工业级精度控制：所有金融计算精确到小数点后两位
 *
 * 设计规范：
 * - 价格类字段：Decimal(18,2)（最大支持千亿级数值，精确到分）
 * - 百分比字段：Decimal(10,2)（范围±999999.99%，满足金融场景需求）
 * - 缓冲区设置：支撑位+2%，压力位-2%（防止价格波动误判）
 */
object Bus_5_AdvancedDipStrategy {

  // 输入数据结构（全量压力支撑位数据）
  case class StockData(
                        // 证券唯一标识（格式示例：SH600000）
                        stock_code: String,
                        // 交易时间戳（格式：yyyy-MM-dd HH:mm:ss）
                        trade_time: String,
                        // 当日收盘价（单位：元，精度0.01）
                        close: BigDecimal,
                        // MA压力位（20日均价+1倍标准差）
                        ma_pressure: BigDecimal,
                        // MA支撑位（20日均价-1倍标准差）
                        ma_support: BigDecimal,
                        // 布林带上轨（20日中轨+2倍标准差）
                        boll_pressure: BigDecimal,
                        // 布林带下轨（20日中轨-2倍标准差）
                        boll_support: BigDecimal,
                        // 枢轴点压力位（标准PP计算法）
                        pivot_pressure: BigDecimal,
                        // 枢轴点支撑位（标准PP计算法）
                        pivot_support: BigDecimal,
                        // 前高压力位（近20交易日最高价）
                        high_low_pressure: BigDecimal,
                        // 前低支撑位（近20交易日最低价）
                        high_low_support: BigDecimal,
                        // 当日成交量（单位：股）
                        volume: Int,
                        //窗口大小
                        windowSize:Int
                      )

  // 增强分析结果结构
  case class PrecisionResult(
                              // 证券代码（与输入一致）
                              stock_code: String,
                              // 触发分析的时间点
                              trade_time: String,
                              // 当前收盘价（直接取自输入）
                              close: BigDecimal,
                              /**
                               * 【复合支撑位】计算逻辑：
                               * LEAST(ma_support, boll_support, pivot_support, high_low_support)
                               * 业务意义：取最保守（最低）的支撑位，确保策略安全边际
                               * 示例：当MA支撑=10.00，前低支撑=9.80 → 取9.80
                               */
                              composite_support: BigDecimal,
                              /**
                               * 【复合压力位】计算逻辑：
                               * GREATEST(ma_pressure, boll_pressure, pivot_pressure, high_low_pressure)
                               * 业务意义：取最激进（最高）的压力位，反映最大潜在空间
                               * 示例：当布林压力=12.50，前高压=12.80 → 取12.80
                               */
                              composite_pressure: BigDecimal,
                              /**
                               * 【支撑位比率】计算逻辑：
                               * (close / composite_support) * 100
                               * 业务意义：当前价格相对于支撑位的位置
                               * - >100%：价格在支撑位上方
                               * - <100%：价格跌破支撑位
                               * 示例：98.50表示低于支撑位1.5%
                               */
                              support_ratio: BigDecimal,
                              /**
                               * 【压力位比率】计算逻辑：
                               * (close / composite_pressure) * 100
                               * 业务意义：当前价格相对于压力位的位置
                               * - >100%：突破压力位（需结合成交量分析）
                               * - <100%：在压力位下方
                               * 示例：102.30表示突破压力位2.3%
                               */
                              pressure_ratio: BigDecimal,
                              /**
                               * 【通道位置】计算逻辑：
                               * ((close - support) / (pressure - support)) * 100
                               * 业务意义：当前价格在支撑压力通道中的相对位置
                               * - 0%：位于支撑位
                               * - 100%：触及压力位
                               * - 35.25：位于支撑到压力的35.25%位置
                               */
                              channel_position: BigDecimal,
                              /**
                               * 【有效支撑类型】识别逻辑：
                               * 当价格在支撑位±2%范围内时，标记对应支撑类型
                               * 格式：类型用|分隔，如"MA支撑|前低支撑"
                               */
                              support_types: String,
                              /**
                               * 【有效压力类型】识别逻辑：
                               * 当价格在压力位±2%范围内时，标记对应压力类型
                               * 格式：类型用|分隔，如"布林带压力|前高压力"
                               */
                              pressure_types: String,
                              windowSize:Int
                            )

  def main(args: Array[String]): Unit = {
    // 创建Spark环境（生产级配置）
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      //      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.shuffle.partitions", "10")
      .config("spark.driver.memory","8g")
      // 增加JDBC并行任务数
      .config("spark.jdbc.parallelism", "10")
      .config("spark.local.dir","D:\\SparkTemp")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  // 高效序列化
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

    spark.sparkContext.setLogLevel("ERROR")  // 屏蔽非关键日志
    val outputPath = "file:///D:\\gsdata\\pressure_support_calculator\\valid_results_pressure_advanced_dip_strategy"
    import spark.implicits._

    // 阶段0：数据加载与清洗
    val stockDF  = spark.read.jdbc(url, "pressure_support_calculator2025", properties)
      .union(spark.read.jdbc(url, "pressure_support_calculator2024", properties))
      .union(spark.read.jdbc(url, "pressure_support_calculator2023", properties))
      .union(spark.read.jdbc(url, "pressure_support_calculator2022", properties))
      .union(spark.read.jdbc(url, "pressure_support_calculator2021", properties))
      .union(spark.read.jdbc(url, "pressure_support_calculator2020", properties))
      .union(spark.read.jdbc(url, "pressure_support_calculator2019", properties))
      .union(spark.read.jdbc(url, "pressure_support_calculator2018", properties))
      .union(spark.read.jdbc(url, "pressure_support_calculator2017", properties))
      .union(spark.read.jdbc(url, "pressure_support_calculator2016", properties))
      .union(spark.read.jdbc(url, "pressure_support_calculator2015", properties))

      .distinct()
      .select(
        // 显式类型转换确保数据质量
        $"stock_code".cast(StringType),  // 证券代码标准化
        $"trade_time".cast(StringType),  // 时间字段统一为字符串
        // 价格类字段统一精度处理
        $"close".cast(DecimalType(18, 2)),
        $"ma_pressure".cast(DecimalType(18, 2)),
        $"ma_support".cast(DecimalType(18, 2)),
        $"boll_pressure".cast(DecimalType(18, 2)),
        $"boll_support".cast(DecimalType(18, 2)),
        $"pivot_pressure".cast(DecimalType(18, 2)),
        $"pivot_support".cast(DecimalType(18, 2)),
        $"high_low_pressure".cast(DecimalType(18, 2)),
        $"high_low_support".cast(DecimalType(18, 2)),
        // 成交量转为整数（避免小数股数）
        $"volume".cast(IntegerType),
        $"windowSize".cast(IntegerType)
      ).as[StockData]

    // 阶段1：复合位计算（工业级精度控制）
    val processedDF = stockDF
      /**
       * 计算逻辑说明：
       * 使用LEAST函数取四类支撑位中的最小值，确保最保守的支撑位
       * 显式转换为Decimal(18,2)防止自动类型推导问题
       */
      .withColumn("composite_support",
        least(
          $"ma_support",        // MA支撑
          $"boll_support",      // 布林带支撑
          $"pivot_support",     // 枢轴支撑
          $"high_low_support"   // 前低支撑
        ).cast(DecimalType(18, 2))
      )
      /**
       * 计算逻辑说明：
       * 使用GREATEST函数取四类压力位中的最大值，反映最大上行空间
       */
      .withColumn("composite_pressure",
        greatest(
          $"ma_pressure",       // MA压力
          $"boll_pressure",     // 布林压力
          $"pivot_pressure",    // 枢轴压力
          $"high_low_pressure"  // 前高压力
        ).cast(DecimalType(18, 2))
      )

    // 阶段2：价格位置分析（精确量化）
    val analyzedDF = processedDF
      /**
       * 支撑比率计算要点：
       * 1. 先进行除法运算，再乘以100转为百分比
       * 2. 使用Decimal(10,2)存储结果（支持-999999.99%~999999.99%）
       */
      .withColumn("support_ratio", (($"close" / $"composite_support") * 100).cast(DecimalType(10, 2)))
      /**
       * 压力比率计算要点：
       * 当压力位为0时自动返回null（内置异常处理）
       */
      .withColumn("pressure_ratio", (($"close" / $"composite_pressure") * 100).cast(DecimalType(10, 2)))
      /**
       * 通道位置计算注意事项：
       * 1. 当支撑=压力时返回null（避免除零错误）
       * 2. 结果标准化到0%~100%范围
       */
      .withColumn("channel_position",
        when($"composite_pressure" =!= $"composite_support",
          ((($"close" - $"composite_support") /
            ($"composite_pressure" - $"composite_support")) * 100)
            .cast(DecimalType(10, 2))
        )
      )

    // 阶段3：动态类型识别（带缓冲区机制）
    val finalDF = analyzedDF
      /**
       * 支撑类型识别规则：
       * - 价格低于支撑位的102%（2%缓冲）
       * - 多个满足条件时用|分隔
       * 示例：当价格在MA支撑1.5%范围内→标记"MA支撑"
       */
      .withColumn("support_types", concat_ws("|",
        when($"close" < ($"ma_support" * 1.02).cast(DecimalType(18,2)), "MA支撑"),
        when($"close" < ($"boll_support" * 1.02).cast(DecimalType(18,2)), "布林带支撑"),
        when($"close" < ($"pivot_support" * 1.02).cast(DecimalType(18,2)), "枢轴支撑"),
        when($"close" < ($"high_low_support" * 1.02).cast(DecimalType(18,2)), "前低支撑")
      ))
      /**
       * 压力类型识别规则：
       * - 价格高于压力位的98%（2%缓冲）
       * - 支持多重压力识别
       */
      .withColumn("pressure_types", concat_ws("|",
        when($"close" > ($"ma_pressure" * 0.98).cast(DecimalType(18,2)), "MA压力"),
        when($"close" > ($"boll_pressure" * 0.98).cast(DecimalType(18,2)), "布林带压力"),
        when($"close" > ($"pivot_pressure" * 0.98).cast(DecimalType(18,2)), "枢轴压力"),
        when($"close" > ($"high_low_pressure" * 0.98).cast(DecimalType(18,2)), "前高压力")
      ))
      .withColumn("trade_time", date_format(to_timestamp($"trade_time", "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd"))
      .as[PrecisionResult]

    // 结果输出与验证
    finalDF.write.mode("overwrite").parquet(outputPath)
    println("===== 低吸策略分析结果样例 =====")
    finalDF
//      .where("trade_time='2025-02-11'")
//      .orderBy(col("channel_position"))
      .show(20, truncate = false)  // 展示非截断结果
      finalDF
//        .where("trade_time='2025-02-11'")
        .groupBy($"windowSize").agg(count($"windowSize")).show()


    spark.stop()
  }
}