package sparktask.test


import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test2 {

  // 基础窗口定义（按股票代码和时间分区）
  private val stockWindow = Window.partitionBy("symbol").orderBy("date").rowsBetween(-100, 0)

  /** 计算ATR（平均真实波幅） */
  def calculateATR(df: DataFrame, windowDays: Int = 14): DataFrame = {
    val tr = when(col("high") - col("low") > abs(col("high") - lag(col("close"), 1).over(stockWindow)),
      col("high") - col("low"))
      .when(abs(col("high") - lag(col("close"), 1).over(stockWindow)) > abs(col("low") - lag(col("close"), 1).over(stockWindow)),
        abs(col("high") - lag(col("close"), 1).over(stockWindow)))
      .otherwise(abs(col("low") - lag(col("close"), 1).over(stockWindow)))

    df.withColumn("tr", tr)
      .withColumn("atr", avg(col("tr")).over(stockWindow.rowsBetween(-windowDays+1, 0)))
  }

  /** 动态波动率窗口计算 */
  def adaptiveWindow(df: DataFrame, maxWindow: Int = 30): DataFrame = {
    // 获取市场基准波动率（全市场ATR中位数）
    val baselineVolatility = df.stat.approxQuantile("atr", Array(0.5), 0.01).head

    df.withColumn("adaptive_window",
      least(lit(maxWindow), greatest(lit(5),
        round(lit(baselineVolatility) / col("atr") * 10).cast("int")))
    )
  }

  /** 计算支撑位和压力位 */
  def calculateSupportResistance(df: DataFrame): DataFrame = {
    val supportExpr = expr("min(low) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN adaptive_window PRECEDING AND CURRENT ROW)")
    val resistanceExpr = expr("max(high) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN adaptive_window PRECEDING AND CURRENT ROW)")

    df.withColumn("support", supportExpr)
      .withColumn("resistance", resistanceExpr)
      .withColumn("risk_ratio", (col("close") - col("support")) / (col("resistance") - col("support") + 1e-9))
  }

  /** 风险控制信号生成 */
  def generateSignals(df: DataFrame): DataFrame = {
    df.withColumn("signal",
      when(col("close") > col("resistance"), 1)    // 突破压力位买入
        .when(col("close") < col("support"), -1)   // 跌破支撑位卖出
        .otherwise(0)
    )
  }

  /** 参数优化流水线 */
  def optimizeParameters(df: DataFrame, maxWindowRange: Seq[Int] = 10 to 30, atrWindowRange: Seq[Int] = 10 to 20): Unit = {
    // 使用遗传算法优化参数（生产环境需对接MLlib）
    maxWindowRange.foreach { maxWindow =>
      atrWindowRange.foreach { atrWindow =>
        val processedDF = df.transform(calculateATR(_, atrWindow))
          .transform(adaptiveWindow(_, maxWindow))
          .transform(calculateSupportResistance)
        // 执行历史回测并评估收益风险比（需实现评估指标）
//        evaluateStrategy(processedDF, maxWindow, atrWindow)
      }
    }
  }

  // 示例数据加载
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SupportResistance").getOrCreate()

    // 加载原始数据（需包含symbol, date, open, high, low, close）
    val rawDF = spark.read.parquet("hdfs://path/to/stock_data")

    // 计算支撑压力位
    val processedDF = rawDF.transform(calculateATR(_))
      .transform(adaptiveWindow(_))
      .transform(calculateSupportResistance)
      .transform(generateSignals)

    // 持久化结果
    processedDF.write.mode("overwrite").parquet("hdfs://path/to/result")
  }
}
