package spark.bus.bond

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.ParameterSet
import spark.tools.MysqlProperties
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import scala.collection.mutable.ArrayBuffer

/**
 *
 * 转债数据转parquet
 *
 */
object Bond2mysql {

  // 默认使用20%作为涨跌幅限制（债券标准）
  val DEFAULT_LIMIT_UP_RATIO = 1.20
  val DEFAULT_LIMIT_DOWN_RATIO = 0.80
  val DEFAULT_TABLE_NAME = "data_bond_daily"
  val DEFAULT_TABLE_NAME_ODS = "data_bond_daily_ods"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions","10")
      .set("spark.driver.memory","6g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "10")
      .set("spark.local.dir","D:\\SparkTemp")

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val properties = MysqlProperties.getMysqlProperties()
    val url = properties.getProperty("url")

    val startm = System.currentTimeMillis()

    val result_df = updateBond(spark,properties)
//    result_df.where("bond_code=''").show()
    println(result_df.count)
    result_df.write.mode("overwrite").jdbc(url, DEFAULT_TABLE_NAME_ODS, properties)

    val endm = System.currentTimeMillis()
    println("共耗时："+(endm-startm)/1000+"秒")
    spark.close()
  }


  def calculateIndicators(df: DataFrame): DataFrame = {
    calculateIndicatorsWithCustomLimit(df, DEFAULT_LIMIT_UP_RATIO, DEFAULT_LIMIT_DOWN_RATIO)
  }

  def calculateIndicatorsWithCustomLimit(
                                          df: DataFrame,
                                          limitUpRatio: Double = DEFAULT_LIMIT_UP_RATIO,
                                          limitDownRatio: Double = DEFAULT_LIMIT_DOWN_RATIO
                                        ): DataFrame = {

    // 定义窗口函数，按bond_code分区，按date排序
    val windowSpec = Window.partitionBy("bond_code").orderBy("date")

    // 计算前收盘价
    val dfWithPreClose = df.withColumn("pre_close", lag("close", 1).over(windowSpec))

    // 计算涨幅相关指标
    val dfWithZf = dfWithPreClose
      .withColumn("kpzf",
        when(col("pre_close").isNotNull && col("pre_close") =!= 0,
          round((col("open") - col("pre_close")) / col("pre_close") * 100, 4)
        ).otherwise(null)
      )
      .withColumn("spzf",
        when(col("pre_close").isNotNull && col("pre_close") =!= 0,
          round((col("close") - col("pre_close")) / col("pre_close") * 100, 4)
        ).otherwise(null)
      )
      .withColumn("zgzf",
        when(col("pre_close").isNotNull && col("pre_close") =!= 0,
          round((col("high") - col("pre_close")) / col("pre_close") * 100, 4)
        ).otherwise(null)
      )
      .withColumn("zdzf",
        when(col("pre_close").isNotNull && col("pre_close") =!= 0,
          round((col("low") - col("pre_close")) / col("pre_close") * 100, 4)
        ).otherwise(null)
      )

    // 计算涨停跌停相关指标（债券通常为20%涨跌幅限制）
    val dfWithZtDt = dfWithZf
      // 当日是否涨停（收盘价达到涨停价）
      .withColumn("sfzt",
        when(col("pre_close").isNotNull && col("pre_close") =!= 0,
          col("close") >= col("pre_close") * limitUpRatio
        ).otherwise(false)
      )
      // 当日是否有过涨停（最高价达到过涨停价）
      .withColumn("cjzt",
        when(col("pre_close").isNotNull && col("pre_close") =!= 0,
          col("high") >= col("pre_close") * limitUpRatio
        ).otherwise(false)
      )
      // 当日是否跌停（收盘价达到跌停价）
      .withColumn("sfdt",
        when(col("pre_close").isNotNull && col("pre_close") =!= 0,
          col("close") <= col("pre_close") * limitDownRatio
        ).otherwise(false)
      )
      // 当日是否有过跌停（最低价达到过跌停价）
      .withColumn("cjdt",
        when(col("pre_close").isNotNull && col("pre_close") =!= 0,
          col("low") <= col("pre_close") * limitDownRatio
        ).otherwise(false)
      )

    // 计算其他指标
    val dfWithOtherIndicators = dfWithZtDt
      // 区间涨幅（收盘相对开盘的涨幅）
      .withColumn("qjzf",
        when(col("open").isNotNull && col("open") =!= 0,
          round((col("close") - col("open")) / col("open") * 100, 4)
        ).otherwise(null)
      )
      // 实体涨幅（使用前收盘价作为基准，收盘相对于开盘的变化率）
      .withColumn("stzf",
        when(col("pre_close").isNotNull && col("pre_close") =!= 0,
          round((col("close") - col("open")) / col("pre_close") * 100, 4)
        ).otherwise(null)
      )

    // 重新排列列的顺序
    dfWithOtherIndicators.select(
      "date", "open", "close", "high", "low", "volume", "bond_code","stock_code","bond_code_2",
      "pre_close", "sfzt", "cjzt", "sfdt", "cjdt",
      "kpzf", "spzf", "zgzf", "zdzf", "qjzf", "stzf"
    )
  }

  // 处理特殊情况：某些债券可能有不同的涨跌幅限制
  def calculateIndicatorsWithDynamicLimit(
                                           df: DataFrame,
                                           bondLimitMap: Map[String, (Double, Double)] = Map.empty
                                         ): DataFrame = {

    val windowSpec = Window.partitionBy("bond_code").orderBy("date")

    val dfWithPreClose = df.withColumn("pre_close", lag("close", 1).over(windowSpec))

    // 创建UDF用于动态获取涨跌幅限制
    val getLimitUpRatio = udf((bondCode: String) => {
      bondLimitMap.get(bondCode).map(_._1).getOrElse(DEFAULT_LIMIT_UP_RATIO)
    })

    val getLimitDownRatio = udf((bondCode: String) => {
      bondLimitMap.get(bondCode).map(_._2).getOrElse(DEFAULT_LIMIT_DOWN_RATIO)
    })

    val dfWithLimits = dfWithPreClose
      .withColumn("limit_up_ratio", getLimitUpRatio(col("bond_code")))
      .withColumn("limit_down_ratio", getLimitDownRatio(col("bond_code")))

    val dfWithZf = dfWithLimits
      .withColumn("kpzf",
        when(col("pre_close").isNotNull && col("pre_close") =!= 0,
          round((col("open") - col("pre_close")) / col("pre_close") * 100, 4)
        ).otherwise(null)
      )
      .withColumn("spzf",
        when(col("pre_close").isNotNull && col("pre_close") =!= 0,
          round((col("close") - col("pre_close")) / col("pre_close") * 100, 4)
        ).otherwise(null)
      )
      .withColumn("zgzf",
        when(col("pre_close").isNotNull && col("pre_close") =!= 0,
          round((col("high") - col("pre_close")) / col("pre_close") * 100, 4)
        ).otherwise(null)
      )
      .withColumn("zdzf",
        when(col("pre_close").isNotNull && col("pre_close") =!= 0,
          round((col("low") - col("pre_close")) / col("pre_close") * 100, 4)
        ).otherwise(null)
      )

    // 使用动态的涨跌幅限制
    val dfWithZtDt = dfWithZf
      .withColumn("sfzt",
        when(col("pre_close").isNotNull && col("pre_close") =!= 0,
          col("close") >= col("pre_close") * col("limit_up_ratio")
        ).otherwise(false)
      )
      .withColumn("cjzt",
        when(col("pre_close").isNotNull && col("pre_close") =!= 0,
          col("high") >= col("pre_close") * col("limit_up_ratio")
        ).otherwise(false)
      )
      .withColumn("sfdt",
        when(col("pre_close").isNotNull && col("pre_close") =!= 0,
          col("close") <= col("pre_close") * col("limit_down_ratio")
        ).otherwise(false)
      )
      .withColumn("cjdt",
        when(col("pre_close").isNotNull && col("pre_close") =!= 0,
          col("low") <= col("pre_close") * col("limit_down_ratio")
        ).otherwise(false)
      )

    val dfWithOtherIndicators = dfWithZtDt
      .withColumn("qjzf",
        when(col("open").isNotNull && col("open") =!= 0,
          round((col("close") - col("open")) / col("open") * 100, 4)
        ).otherwise(null)
      )
      .withColumn("stzf",
        when(col("pre_close").isNotNull && col("pre_close") =!= 0,
          round((col("close") - col("open")) / col("pre_close") * 100, 4)
        ).otherwise(null)
      )

    dfWithOtherIndicators.select(
      "date", "open", "close", "high", "low", "volume", "bond_code","stock_code","bond_code_2",
      "pre_close", "sfzt", "cjzt", "sfdt", "cjdt",
      "kpzf", "spzf", "zgzf", "zdzf", "qjzf", "stzf"
    )
  }

  // 增强版本：添加更多债券相关的技术指标
  def calculateEnhancedIndicators(df: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("bond_code").orderBy("date")

    val baseDF = calculateIndicators(df)

    // 添加更多技术指标
    val enhancedDF = baseDF
      // 计算振幅（最高-最低）/前收盘
      .withColumn("zf",
        when(col("pre_close").isNotNull && col("pre_close") =!= 0,
          round((col("high") - col("low")) / col("pre_close") * 100, 4)
        ).otherwise(null)
      )
      // 计算换手率（需要流通股本数据，这里用volume作为占位）
      .withColumn("turnover", col("volume")) // 实际计算需要更多数据
      // 计算5日均线
      .withColumn("ma5",
        avg(col("close")).over(windowSpec.rowsBetween(-4, 0))
      )
      // 计算10日均线
      .withColumn("ma10",
        avg(col("close")).over(windowSpec.rowsBetween(-9, 0))
      )
      // 计算20日均线
      .withColumn("ma20",
        avg(col("close")).over(windowSpec.rowsBetween(-19, 0))
      )
      // 计算20日均线
      .withColumn("ma60",
        avg(col("close")).over(windowSpec.rowsBetween(-59, 0))
      )
      // 计算120日均线
      .withColumn("ma120",
        avg(col("close")).over(windowSpec.rowsBetween(-119, 0))
      )
      // 计算涨跌停价格
      .withColumn("limit_up_price",
        when(col("pre_close").isNotNull,
          round(col("pre_close") * DEFAULT_LIMIT_UP_RATIO, 2)
        ).otherwise(null)
      )
      .withColumn("limit_down_price",
        when(col("pre_close").isNotNull,
          round(col("pre_close") * DEFAULT_LIMIT_DOWN_RATIO, 2)
        ).otherwise(null)
      )
      // 判断是否连续涨跌停
      .withColumn("prev_sfzt", lag("sfzt", 1).over(windowSpec))
      .withColumn("prev_sfdt", lag("sfdt", 1).over(windowSpec))
      .withColumn("consecutive_up_limit",
        col("sfzt") && col("prev_sfzt")
      )
      .withColumn("consecutive_down_limit",
        col("sfdt") && col("prev_sfdt")
      )
      .drop("prev_sfzt", "prev_sfdt")

    enhancedDF
  }


  /**
   * 按天更新股票数据
   *
   */
  def updateBond(spark: SparkSession,properties: Properties): DataFrame = {
    val url = properties.getProperty("url")

    var df:DataFrame = spark.read.jdbc(url, DEFAULT_TABLE_NAME, properties)
        .withColumn("volume", col("volume").cast(IntegerType))
        .withColumn("open", col("open").cast(DecimalType(10, 2)))
        .withColumn("close", col("close").cast(DecimalType(10, 2)))
        .withColumn("high", col("high").cast(DecimalType(10, 2)))
        .withColumn("low", col("low").cast(DecimalType(10, 2)))

    val df2 = calculateEnhancedIndicators(df: DataFrame)
    df2
  }

}
