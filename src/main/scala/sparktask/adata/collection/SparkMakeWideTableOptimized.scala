package sparktask.adata.collection

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

/**
 * 优化版宽表生成
 * 改进：
 * 1. 使用窗口函数（lag/lead）替代10次自连接，减少Shuffle
 * 2. 动态计算月份分区数，按月份哈希分区写入
 * 3. 精简缓存，仅缓存必要的中间结果
 * 4. 移除row_number中冗余的ORDER BY字段
 */
object SparkMakeWideTableOptimized {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")               // 改用 lz4
      .set("spark.shuffle.io.maxRetries", "5")                // 增加重试
      .set("spark.shuffle.io.retryWait", "10s")
      .set("spark.sql.shuffle.partitions", "800")              // 增加分区数
      .set("spark.shuffle.checksum.enabled", "true")           // 启用校验和
      .set("spark.driver.memory", "12g")
      .set("spark.local.dir", "D:\\SparkTemp")                 // 确保空间充足
      // 可选：增加 shuffle 内存
      .set("spark.memory.fraction", "0.8")
      .set("spark.memory.storageFraction", "0.3")

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val startTime = System.currentTimeMillis()

    make_wide_all_data(spark)

    val endTime = System.currentTimeMillis()
    println(s"共耗时：${(endTime - startTime) / 1000}秒")
    spark.close()
  }

  def make_wide_all_data(spark: SparkSession): Unit = {
    // 1. 读取原始Parquet数据
    val df2 = spark.read.parquet("file:///D:\\gsdata3\\gpsj_day_all_hs")


    // 2. 生成带行号和前一天特征的中间表 df4
    val originalCols = df2.columns
    val windowSpec = "PARTITION BY stock_code ORDER BY trade_date"
    val withLagDF = df2.selectExpr(
      originalCols.map(c => s"`$c`") ++ Seq(
        s"row_number() OVER ($windowSpec) AS row",
        s"LAG(high) OVER ($windowSpec) AS prev_high",
        s"LAG(low) OVER ($windowSpec) AS prev_low",
        s"LAG(volume) OVER ($windowSpec) AS prev_volume"
      ): _*
    )

    val df4 = withLagDF
      .where("prev_high IS NOT NULL")
      .selectExpr(
        (originalCols :+ "row").map(c => s"`$c`") ++ Seq(
          """
            |CASE WHEN high < prev_high THEN '下探'
            |     WHEN high = prev_high THEN '平整'
            |     ELSE '突破'
            |END AS sx
        """.stripMargin,
          """
            |CASE WHEN low < prev_low THEN '下探'
            |     WHEN low = prev_low THEN '平整'
            |     ELSE '突破'
            |END AS xx
        """.stripMargin,
          """
            |CASE WHEN volume < prev_volume THEN '缩量'
            |     WHEN volume = prev_volume THEN '平量'
            |     WHEN volume / prev_volume > 2 THEN '放巨量'
            |     ELSE '放量'
            |END AS ln
        """.stripMargin,
          "ROUND(volume / prev_volume, 2) AS zrlnb"
        ): _*
      )

    df4.cache()
    df4.createOrReplaceTempView("ta4")

    // 3. 使用lead窗口函数一次性获取未来9天的所有字段
    val allCols = df4.columns
    val t0Exprs = allCols.map(c => s"`$c` AS t0_$c")
    val leadExprs = (1 to 9).flatMap { i =>
      allCols.map { c =>
        s"LEAD(`$c`, $i) OVER (PARTITION BY stock_code ORDER BY row) AS t${i}_$c"
      }
    }
    val df5 = df4.selectExpr(t0Exprs ++ leadExprs: _*)

    // 添加月份列（用于分区）
    val df5WithMonth = df5.withColumn("trade_date_month", substring(col("t0_trade_date"), 0, 7))

    // 4. 删除所有包含 "_row" 的临时列（t0_row ~ t9_row）
    val rowColumns = df5WithMonth.columns.filter(_.contains("_row")).union(df5WithMonth.columns.filter(_.contains("_trade_time")))
    val dfNoRow = df5WithMonth.drop(rowColumns: _*)

    // 5. 处理 stock_code：保留 t0_stock_code 并重命名为 stock_code，删除其他 t*_stock_code
    val dfWithStockRenamed = dfNoRow.withColumnRenamed("t0_stock_code", "stock_code")
    val otherStockCols = dfWithStockRenamed.columns.filter(c => c.matches("t[1-9]_stock_code"))
      .union(dfWithStockRenamed.columns.filter(c => c.matches("t[0-9]_trade_date_month")))
    val dfFinal = dfWithStockRenamed.drop(otherStockCols: _*)

    // 可选：打印最终 schema 以确认
    println("Final schema:")
//    dfFinal.where("stock_code='600173' and t0_trade_date='2025-12-31'").show()
//    dfFinal.printSchema()

    // 6. 动态计算月份分区数并写入
    val monthCount = dfFinal.select("trade_date_month").distinct().count().toInt
    val numPartitions = math.max(monthCount * 2, 1)

    dfFinal
      .repartition(numPartitions, col("trade_date_month"))
      .write
      .mode("overwrite")
      .partitionBy("trade_date_month")
      .parquet("file:///D:\\gsdata3\\gpsj_hs_10days")

    df4.unpersist()
  }
}