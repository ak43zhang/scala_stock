//import org.apache.spark.sql.{SparkSession, DataFrame}
//import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types._
//import org.apache.spark.ml.Pipeline
//import org.apache.spark.ml.feature.VectorAssembler
//import org.apache.spark.ml.evaluation.RegressionEvaluator
//import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
//
//object StockSupportResistance {
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder()
//      .appName("DynamicWindowPressureSupportCalculation")
//      .config("spark.sql.shuffle.partitions", "200")
//      .getOrCreate()
//
//    import spark.implicits._
//
//    // 1. 数据加载和预处理
//    val rawData = spark.read.parquet("hdfs://path/to/stock_data")
//      .filter($"high" > $"low" && $"volume" > 0) // 基础数据清洗
//
//    // 2. 数据降噪处理
//    val cleanedData = denoiseData(rawData)
//
//    // 3. 特征工程：ATR计算
//    val atrData = calculateATR(cleanedData, 14)
//
//    // 4. 动态窗口计算
//    val windowData = calculateDynamicWindow(atrData)
//
//    // 5. 多方法压力支撑计算
//    val fullFeatureData = calculateAllMethods(windowData)
//
//    // 6. 参数优化流水线
//    val optimizedParams = optimizeParameters(fullFeatureData)
//
//    // 7. 最佳参数应用
//    val finalResult = applyBestParameters(fullFeatureData, optimizedParams)
//
//    // 8. 结果持久化
//    finalResult.write.mode("overwrite").parquet("hdfs://path/to/result")
//
//    spark.stop()
//  }
//
//  // 数据降噪方法
//  private def denoiseData(df: DataFrame): DataFrame = {
//    df.withColumn("price_range", $"high" - $"low")
//      .withColumn("median_volume", median($"volume").over(Window.partitionBy($"symbol")))
//      .filter($"price_range" < 2 * stddev($"price_range").over(Window.partitionBy($"symbol")))
//      .filter($"volume" < 5 * $"median_volume")
//      .drop("price_range", "median_volume")
//  }
//
//  // ATR计算方法
//  private def calculateATR(df: DataFrame, period: Int): DataFrame = {
//    val windowSpec = Window.partitionBy($"symbol").orderBy($"date").rowsBetween(-period, 0)
//
//    df.withColumn("prev_close", lag($"close", 1).over(Window.partitionBy($"symbol").orderBy($"date")))
//      .withColumn("tr1", $"high" - $"low")
//      .withColumn("tr2", abs($"high" - $"prev_close"))
//      .withColumn("tr3", abs($"low" - $"prev_close"))
//      .withColumn("tr", greatest($"tr1", $"tr2", $"tr3"))
//      .withColumn("atr", avg($"tr").over(windowSpec))
//      .drop("tr1", "tr2", "tr3", "prev_close")
//  }
//
//  // 动态窗口计算
//  private def calculateDynamicWindow(df: DataFrame): DataFrame = {
//    val volatilityWindow = Window.partitionBy($"symbol").orderBy($"date").rowsBetween(-30, 0)
//
//    df.withColumn("volatility", stddev($"close").over(volatilityWindow) / avg($"close").over(volatilityWindow))
//      .withColumn("dynamic_window",
//        when($"volatility" < 0.01, 20)
//          .when($"volatility" < 0.03, 14)
//          .when($"volatility" < 0.05, 10)
//          .otherwise(5))
//  }
//
//  // 多方法压力支撑计算
//  private def calculateAllMethods(df: DataFrame): DataFrame = {
//    val methods = List("pivot", "ma", "bollinger", "fhl")
//
//    methods.foldLeft(df)((acc, method) =>
//      method match {
//        case "pivot" => calculatePivotPoints(acc)
//        case "ma" => calculateMA(acc)
//        case "bollinger" => calculateBollinger(acc)
//        case "fhl" => calculateFHL(acc)
//      })
//  }
//
//  // 枢轴点法
//  private def calculatePivotPoints(df: DataFrame): DataFrame = {
//    df.withColumn("pivot", ($"high" + $"low" + $"close") / 3)
//      .withColumn("r1", 2 * $"pivot" - $"low")
//      .withColumn("s1", 2 * $"pivot" - $"high")
//  }
//
//  // 移动平均法
//  private def calculateMA(df: DataFrame): DataFrame = {
//    df.withColumn("ma_support", avg($"low").over(Window.partitionBy($"symbol")
//      .orderBy($"date").rowsBetween(-5, 0)))
//      .withColumn("ma_resistance", avg($"high").over(Window.partitionBy($"symbol")
//        .orderBy($"date").rowsBetween(-5, 0)))
//  }
//
//  // 布林带法
//  private def calculateBollinger(df: DataFrame): DataFrame = {
//    val window = Window.partitionBy($"symbol").orderBy($"date").rowsBetween(-20, 0)
//
//    df.withColumn("middle_band", avg($"close").over(window))
//      .withColumn("stddev", stddev($"close").over(window))
//      .withColumn("upper_band", $"middle_band" + 2 * $"stddev")
//      .withColumn("lower_band", $"middle_band" - 2 * $"stddev")
//  }
//
//  // 前高前低法
//  private def calculateFHL(df: DataFrame): DataFrame = {
//    df.withColumn("prev_high", max($"high").over(Window.partitionBy($"symbol")
//      .orderBy($"date").rowsBetween(-10, -1)))
//      .withColumn("prev_low", min($"low").over(Window.partitionBy($"symbol")
//        .orderBy($"date").rowsBetween(-10, -1)))
//  }
//
//  // 参数优化流水线
//  private def optimizeParameters(df: DataFrame): DataFrame = {
//    val featureCols = Array("atr", "volatility", "dynamic_window")
//    val assembler = new VectorAssembler()
//      .setInputCols(featureCols)
//      .setOutputCol("features")
//
//    val rf = new RandomForestRegressor()
//      .setLabelCol("return_ratio")
//      .setFeaturesCol("features")
//      .setNumTrees(20)
//
//    val pipeline = new Pipeline()
//      .setStages(Array(assembler, rf))
//
//    val model = pipeline.fit(df)
//    model.transform(df)
//  }
//
//  // 最佳参数应用
//  private def applyBestParameters(df: DataFrame, optimizedDF: DataFrame): DataFrame = {
//    val bestParams = optimizedDF
//      .groupBy($"symbol")
//      .agg(max($"prediction").as("best_window"))
//
//    df.join(bestParams, Seq("symbol"))
//      .withColumn("final_window",
//        when($"dynamic_window" === $"best_window", $"dynamic_window")
//          .otherwise($"best_window"))
//  }
//}
