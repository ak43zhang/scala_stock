package mysql

import java.util.Properties
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object Parquet2mysql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.rpc.askTimeout", "600s")
      // 调整shuffle分区数，根据数据量调整
      .set("spark.sql.shuffle.partitions", "50")
      .set("spark.driver.memory", "4g")
      // 增加executor内存和并行度
      .set("spark.executor.memory", "2g")
      .set("spark.default.parallelism", "100")
      // 调整序列化器
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "256m")
      // 动态分区配置
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m")
      // 批量写入配置
      .set("spark.sql.execution.arrow.enabled", "true")
      .set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
      .set("spark.local.dir", "D:\\SparkTemp")

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val url = "jdbc:mysql://192.168.0.100:3306/gs?rewriteBatchedStatements=true&useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true"
    val driver = "com.mysql.cj.jdbc.Driver"
    val user = "root"
    val pwd = "123456"

    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", pwd)
    properties.setProperty("url", url)
    properties.setProperty("driver", driver)
    // 关键：设置批量写入参数
    properties.setProperty("rewriteBatchedStatements", "true")
    properties.setProperty("useCompression", "true")
    properties.setProperty("cachePrepStmts", "true")
    properties.setProperty("prepStmtCacheSize", "250")
    properties.setProperty("prepStmtCacheSqlLimit", "2048")
    properties.setProperty("useServerPrepStmts", "true")

    val startm = System.currentTimeMillis()

    val tables = List("analysis_news2027","data_gpsj_day_20260202","data_gsdt","data_jyrl","data_lhb","data_zjlx_history","gbxx","hotbk20_ths","hotstock_east","hotstock_ths","jhsaggg2005","jhsaggg2006","jhsaggg2010","popularity_day","pressure_support_calculator2015","pressure_support_calculator2016","pressure_support_calculator2017","pressure_support_calculator2018","pressure_support_calculator2019","pressure_support_calculator2022","pressure_support_calculator2023","pressure_support_calculatorfor40_2019","pressure_support_calculatorfor40_2020","pressure_support_calculatorfor40_2021","pressure_support_calculatorfor40_2022","pressure_support_calculatorfor40_2023","pressure_support_calculatorfor40_2024","pressure_support_calculatorfor40_2025","pressure_support_calculatorfor40_2026","result_expect_risk2","result_expect_normal","sendemails_news","data_zjlx","ths_gn_bk","wencaiquery_basequery_2015")

    // 方法1：使用并发写入（控制并发数）
    writeTablesConcurrently(spark, tables, url, properties, maxConcurrent = 3)

    // 方法2：或者使用顺序写入但优化每个表的写入
//     writeTablesSequentially(spark, tables, url, properties)

    val endm = System.currentTimeMillis()
    println(s"共耗时：${(endm - startm) / 1000} 秒")
    spark.close()
  }

  /**
   * 并发写入多个表
   */
  def writeTablesConcurrently(spark: SparkSession, tables: List[String], url: String,
                              properties: Properties, maxConcurrent: Int): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val executorService = Executors.newFixedThreadPool(maxConcurrent)
    implicit val ec = ExecutionContext.fromExecutorService(executorService)

    try {
      val futures = tables.map { tableName =>
        Future {
          Try {
            writeTableToMySQL(spark, tableName, url, properties)
          } match {
            case Success(_) =>
              println(s"${tableName} - 写入成功")
            case Failure(e) =>
              println(s"${tableName} - 写入失败")
          }
        }
      }

      // 等待所有任务完成
      Await.result(Future.sequence(futures), Duration.Inf)
    } finally {
      executorService.shutdown()
      executorService.awaitTermination(1, TimeUnit.HOURS)
    }
  }

  /**
   * 顺序写入多个表（更稳定）
   */
  def writeTablesSequentially(spark: SparkSession, tables: List[String],
                              url: String, properties: Properties): Unit = {
    tables.foreach { tableName =>
      try {
        writeTableToMySQL(spark, tableName, url, properties)
        println(s"${tableName} - 写入成功")
      } catch {
        case e: Exception =>
          println(s"${tableName} - 出错: ${e.getMessage}")
          // 可以添加重试逻辑
          retryWriteTable(spark, tableName, url, properties, retries = 3)
      }
    }
  }

  /**
   * 写入单个表到MySQL
   */
  def writeTableToMySQL(spark: SparkSession, tableName: String,
                        url: String, properties: Properties): Unit = {

    println(s"开始处理表: ${tableName}")
    val startTime = System.currentTimeMillis()

    // 读取parquet文件
    val df = spark.read.parquet(s"file:///H:\\mysql_backup\\gs3\\parquet\\$tableName")

    // 统计行数
    val rowCount = df.count()
    println(s"表 ${tableName} 共有 ${rowCount} 行数据")

    // 根据数据量动态调整分区数
    val numPartitions = calculatePartitions(rowCount)

    // 重新分区以优化写入性能
    val repartitionedDf = if (numPartitions > df.rdd.getNumPartitions) {
      df.repartition(numPartitions)
    } else if (rowCount > 1000000) {
      // 对于大数据量，使用coalesce减少分区数，避免创建太多JDBC连接
      df.coalesce(math.min(numPartitions, 50))
    } else {
      df
    }

    // 缓存DataFrame以减少重复计算
    repartitionedDf.cache()

    try {
      // 获取表结构信息
      val columnTypes = getColumnTypes(repartitionedDf)

      // 设置批量写入参数
      val writeOptions = Map(
        "truncate" -> "true",
        "createTableColumnTypes" -> columnTypes,
        "batchsize" -> "50000", // 增加批处理大小
        "isolationLevel" -> "NONE", // 使用无事务提交，提高速度
        "numPartitions" -> numPartitions.toString,
        "fetchsize" -> "10000" // 增加获取大小
      )

      // 写入MySQL
      repartitionedDf.write
        .mode(SaveMode.Overwrite)
        .options(writeOptions)
        .jdbc(url, tableName, properties)

      val endTime = System.currentTimeMillis()
      println(s"表 ${tableName} 写入完成，耗时: ${(endTime - startTime) / 1000} 秒")

    } finally {
      // 释放缓存
      repartitionedDf.unpersist()
    }
  }

  /**
   * 重试写入
   */
  def retryWriteTable(spark: SparkSession, tableName: String, url: String,
                      properties: Properties, retries: Int): Unit = {
    var attempt = 0
    var success = false

    while (attempt < retries && !success) {
      attempt += 1
      try {
        println(s"第${attempt}次重试写入表: ${tableName}")
        Thread.sleep(attempt * 5000) // 指数退避
        writeTableToMySQL(spark, tableName, url, properties)
        success = true
      } catch {
        case e: Exception if attempt < retries =>
          println(s"第${attempt}次重试失败: ${e.getMessage}")
        case e: Exception =>
          println(s"表${tableName}重试${retries}次后仍然失败")
          throw e
      }
    }
  }

  /**
   * 根据数据量计算合适的分区数
   */
  def calculatePartitions(rowCount: Long): Int = {
    if (rowCount < 10000) 1
    else if (rowCount < 100000) 5
    else if (rowCount < 1000000) 10
    else if (rowCount < 10000000) 20
    else 50
  }

  /**
   * 根据DataFrame的schema生成MySQL列类型
   */
  def getColumnTypes(df: DataFrame): String = {
    val columnTypes = df.schema.fields.map { field =>
      val mysqlType = field.dataType match {
        case org.apache.spark.sql.types.IntegerType => "INT"
        case org.apache.spark.sql.types.LongType => "BIGINT"
        case org.apache.spark.sql.types.DoubleType => "DOUBLE"
        case org.apache.spark.sql.types.FloatType => "FLOAT"
        case org.apache.spark.sql.types.StringType =>
          // 根据字段名判断是否需要使用更大的TEXT类型
          if (field.name.toLowerCase.contains("content") ||
            field.name.toLowerCase.contains("text") ||
            field.name.toLowerCase.contains("description")) {
            "LONGTEXT"
          } else {
            "VARCHAR(1000)"
          }
        case org.apache.spark.sql.types.BooleanType => "TINYINT(1)"
        case org.apache.spark.sql.types.DateType => "DATE"
        case org.apache.spark.sql.types.TimestampType => "DATETIME(6)"
        case org.apache.spark.sql.types.DecimalType() => "DECIMAL(20,6)"
        case _ => "TEXT"
      }
      s"${field.name} $mysqlType"
    }

    columnTypes.mkString(", ")
  }

  /**
   * 可选：使用Load Data Infile方式（速度最快，但需要文件权限）
   */
  def writeUsingLoadDataInfile(spark: SparkSession, tableName: String,
                               url: String, properties: Properties): Unit = {
    // 这种方式需要将数据保存为CSV，然后使用MySQL的LOAD DATA INFILE
    // 速度比JDBC快很多，但需要MySQL服务器有文件访问权限
    println(s"警告：Load Data Infile方式需要额外配置，这里只是示例")

    val df = spark.read.parquet(s"file:///H:\\mysql_backup\\gs3\\parquet\\$tableName")

    // 1. 保存为CSV到临时目录
    val tempCsvPath = s"D:\\SparkTemp\\${tableName}_temp.csv"
    df.write
      .option("header", "true")
      .option("delimiter", "|") // 使用不常见的分隔符避免数据冲突
      .option("nullValue", "\\N")
      .csv(tempCsvPath)

    // 2. 使用JDBC执行LOAD DATA INFILE命令
    // 注意：这需要MySQL服务器允许LOAD DATA INFILE，且文件在MySQL服务器可访问的位置
    // 实际实现需要考虑文件传输和权限问题
  }
}