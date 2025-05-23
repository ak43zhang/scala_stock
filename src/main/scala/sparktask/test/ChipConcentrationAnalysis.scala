package sparktask.test
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, _}

object ChipConcentrationAnalysis {
  // 配置常量
  private val WINDOW_SIZE = 30
  private val DECIMAL_PRECISION = 10
  private val DECIMAL_SCALE = 2
  private val BASE_UNIT = 100 // 分单位基准

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
    val inputPath = "file:///D:\\gsdata\\gpsj_day_all_hs\\trade_date_month=2025*"
    val outputPath = "file:///D:\\gsdata\\chip_concentration_analysis"

    // 1. 数据加载与预处理
    val rawDF = spark.read.parquet(inputPath)
      .withColumn("trade_date", col("trade_date").cast(DateType))
      .withColumn("open", col("open").cast(DecimalType(DECIMAL_PRECISION, DECIMAL_SCALE)))
      .withColumn("close", col("close").cast(DecimalType(DECIMAL_PRECISION, DECIMAL_SCALE)))
      .withColumn("high", col("high").cast(DecimalType(DECIMAL_PRECISION, DECIMAL_SCALE)))
      .withColumn("low", col("low").cast(DecimalType(DECIMAL_PRECISION, DECIMAL_SCALE)))
      .withColumn("volume", col("volume").cast(LongType))

    // 2. 计算动态分档
    val withBucketDF = rawDF
      .withColumn("price_range", (col("high") - col("low")).cast(DecimalType(DECIMAL_PRECISION, DECIMAL_SCALE)))
      .withColumn("dynamic_step",
        when(col("price_range") < 5.0, 0.01)
          .when(col("price_range") >= 5.0 && col("price_range") < 20.0, 0.05)
          .otherwise(0.10))
      .withColumn("bucket_start", (col("low") * BASE_UNIT).cast(IntegerType))
      .withColumn("bucket_end", (col("high") * BASE_UNIT).cast(IntegerType))
      .withColumn("step_size", (col("dynamic_step") * BASE_UNIT).cast(IntegerType))

    // 3. 生成价格分桶（修正版本）
    val explodedDF = withBucketDF
      // 添加参数校验
      .filter(col("step_size") > 0)
      .filter(col("bucket_end") >= col("bucket_start"))
      // 展开分桶数组
      .withColumn("price_cent", explode(expr(s"sequence(bucket_start, bucket_end, step_size)")))
      // 精确类型转换
      .select(
        col("stock_code"),
        col("trade_date"),
        (col("price_cent").cast(DecimalType(18,2)) / lit(100).cast(DecimalType(18,2))).as("price_level"),
        col("open"),
        col("close"),
        col("high"),
        col("low"),
        col("volume")
      )
      // 过滤异常值
      .filter(col("price_level").between(0, 100000)) // 假设最大股价10万元

    // 4. 三角权重分配模型
    val weightDF = explodedDF
      .withColumn("distance_to_close", abs(col("price_level") - col("close")))
      .withColumn("max_distance", col("high") - col("low"))
      .withColumn("triangle_weight",
        when(col("max_distance") === 0.0, 1.0)
          .otherwise(lit(1.0) - (col("distance_to_close") / col("max_distance"))))
      .withColumn("weighted_volume", col("volume") * col("triangle_weight"))

    // 5. 时间衰减窗口计算（修正版本）
    val windowSpec = Window.partitionBy("stock_code", "price_level")
      .orderBy(col("trade_date").cast("long").asc)
      .rangeBetween(-WINDOW_SIZE * 86400L, 0)

    val cumulativeDF = weightDF
      .withColumn("current_date", max(col("trade_date")).over(windowSpec))
      .withColumn("days_diff",
        (col("current_date").cast("long") - col("trade_date").cast("long")) / 86400L)
      .withColumn("time_decay", exp(-col("days_diff").cast(DoubleType) / WINDOW_SIZE.toDouble))
      // 关键修复1：处理空值并转换类型
      .withColumn("weighted_value",
        coalesce(col("weighted_volume") * col("time_decay"), lit(0.0)).cast(DecimalType(18, 2)))
      .withColumn("cumulative_volume",
        coalesce(sum(col("weighted_value")).over(windowSpec), lit(0L)).cast(LongType))

    // 6. 计算集中度指标（修复除零错误）
    val resultDF = cumulativeDF
      .groupBy("stock_code", "trade_date")
      .agg(
        sum("cumulative_volume").as("total_volume"),
        sum(col("price_level") * col("cumulative_volume")).as("numerator"),
        first("close").as("close_price")
      )
      // 关键修复2：处理除零异常
      .withColumn("ma_cost", when(col("total_volume") === 0, null)
        .otherwise((col("numerator") / col("total_volume")).cast(DecimalType(DECIMAL_PRECISION, DECIMAL_SCALE))))
        // 关键修复3：使用left join处理空值
        .join(
          cumulativeDF.select(
            col("stock_code"),
            col("trade_date"),
            col("price_level"),
            col("cumulative_volume"),
            row_number().over(Window
              .partitionBy("stock_code", "trade_date")
              .orderBy(col("cumulative_volume").desc)
            ).as("rank")
          ).filter(col("rank") === 1)
            .select(
              col("stock_code"),
              col("trade_date"),
              col("price_level").as("peak_price"),
              col("cumulative_volume").as("peak_volume")
            ),
          Seq("stock_code", "trade_date"),
          "left" // 改为左连接
        )
        // 关键修复4：处理空值比例计算
        .withColumn("concentration_ratio",
          when(col("total_volume") === 0, null)
            .otherwise((col("peak_volume") / col("total_volume"))
              .cast(DecimalType(4, 2))))

    // 7. 数据校验与输出
            resultDF
//      .filter(col("price_level").isNotNull && col("concentration_ratio").between(0, 1))
        .show()
//      .write
//      .mode("overwrite")
//      .parquet(outputPath)

    spark.stop()
  }
}