package mysql

import org.apache.spark.sql.{SparkSession, SaveMode}
import java.sql.{Connection, DriverManager, Statement, ResultSet, DatabaseMetaData}
import java.util.Properties
import java.io.{FileWriter, PrintWriter, File, BufferedReader, FileReader, FilenameFilter}
import com.typesafe.config.ConfigFactory
import scala.collection.mutable
import scala.util.{Try, Success, Failure}
import scala.util.matching.Regex
import scala.collection.JavaConverters._

object MySQLRestore {

  // 恢复策略枚举
  object RestoreStrategy extends Enumeration {
    val OVERWRITE = Value("OVERWRITE")  // 覆盖：删除表后重建
    val APPEND = Value("APPEND")        // 追加：保留现有数据，追加备份数据
    val TRUNCATE = Value("TRUNCATE")    // 清空：保留表结构，清空后加载数据
  }

  case class RestoreError(tableName: String, errorType: String, errorMessage: String, timestamp: Long = System.currentTimeMillis())
  case class RestoreProgress(tableName: String, step: String, status: String, timestamp: Long = System.currentTimeMillis())

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("mysqlbackup.conf")

    val spark = SparkSession.builder()
      .appName(config.getString("spark.appName") + "_Restore")
      .master(config.getString("spark.master"))
      .config("spark.sql.parquet.compression.codec", "snappy")
      .getOrCreate()

    try {
      spark.sparkContext.setLogLevel("WARN")
      restoreDatabase(spark, config)

    } catch {
      case e: Exception =>
        logError("Fatal error in restore process", e, s"${config.getString("backup.basePath")}/${config.getString("restore.errorLog")}")
        throw e
    } finally {
      spark.stop()
    }
  }

  def restoreDatabase(spark: SparkSession, config: com.typesafe.config.Config): Unit = {
    val basePath = config.getString("backup.basePath")
    val ddlDir = s"$basePath/${config.getString("backup.ddlDir")}"
    val dataDir = s"$basePath/${config.getString("backup.dataDir")}"
    val progressFile = s"$basePath/${config.getString("restore.progressFile")}"
    val errorLog = s"$basePath/${config.getString("restore.errorLog")}"
    val skipTablePatterns = config.getStringList("restore.skipTables")
    val restoreStrategyStr = config.getString("restore.restoreStrategy")
    val skipIfTableExists = config.getBoolean("restore.skipIfTableExists")

    val restoreStrategy = RestoreStrategy.withName(restoreStrategyStr)

    println("=" * 80)
    println("MySQL Database Restore Started")
    println("=" * 80)
    println(s"Base path: $basePath")
    println(s"DDL dir: $ddlDir")
    println(s"Data dir: $dataDir")
    println(s"Restore strategy: $restoreStrategy")
    println(s"Skip if table exists: $skipIfTableExists")

    // 检查备份目录是否存在
    val localDdlDir = convertToLocalPath(ddlDir)
    val localDataDir = convertToLocalPath(dataDir)

    val ddlDirFile = new File(localDdlDir)
    val dataDirFile = new File(localDataDir)

    if (!ddlDirFile.exists() || !dataDirFile.exists()) {
      throw new RuntimeException(
        s"""Backup directory does not exist. Please run backup first.
           |DDL dir: $localDdlDir (exists: ${ddlDirFile.exists()})
           |Data dir: $localDataDir (exists: ${dataDirFile.exists()})""".stripMargin)
    }

    // 加载恢复进度
    val restoredTables = loadRestoreProgress(progressFile)

    // 获取DDL文件列表
    val sqlFilter = new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        name.endsWith(".sql")
      }
    }

    val ddlFiles = ddlDirFile.listFiles(sqlFilter)
    if (ddlFiles == null || ddlFiles.isEmpty) {
      println("No DDL files found in backup directory.")
      return
    }

    // 将Java List转换为Scala List
    val skipPatternsList = skipTablePatterns.asScala.toList

    // 转换跳过模式为正则表达式
    val skipRegexes = skipPatternsList.map(pattern =>
      pattern.replace("%", ".*").r
    )

    // 过滤出未恢复的表
    val allTables = ddlFiles.map(_.getName.replace(".sql", "")).toList
    val tablesToRestore = allTables.filter { tableName =>
      // 跳过已经恢复的表
      if (restoredTables.contains(tableName)) {
        false
      } else {
        // 检查是否匹配跳过模式
        val shouldSkip = skipRegexes.exists { regex =>
          regex.findFirstIn(tableName).isDefined
        }

        if (shouldSkip) {
          println(s"Skipping table (matches pattern): $tableName")
          updateRestoreProgress(progressFile, RestoreProgress(tableName, "SKIP", "SKIPPED"))
          false
        } else {
          true
        }
      }
    }

    if (tablesToRestore.isEmpty) {
      println("All tables have been restored or skipped.")
      return
    }

    println(s"Tables to restore: ${tablesToRestore.size}")
    println("\nTables to restore:")
    tablesToRestore.foreach(table => println(s"  - $table"))
    println()

    // 创建目标数据库连接
    val targetProps = new Properties()
    targetProps.put("user", config.getString("mysql.target.user"))
    targetProps.put("password", config.getString("mysql.target.password"))
    targetProps.put("driver", config.getString("mysql.target.driver"))

    val targetUrl = config.getString("mysql.target.url")
    val targetDatabase = extractDatabaseName(targetUrl)

    var connection: Connection = null

    try {
      connection = DriverManager.getConnection(targetUrl, targetProps)

      // 确保使用正确的目标数据库
      val statement = connection.createStatement()
      statement.execute(s"USE `$targetDatabase`")
      statement.close()

      // 按批次并行恢复
      val parallelism = config.getInt("restore.tableParallelism")
      val batches = tablesToRestore.grouped(parallelism).toList

      for ((batch, batchIndex) <- batches.zipWithIndex) {
        println(s"\nProcessing batch ${batchIndex + 1}/${batches.size}")

        batch.par.foreach { tableName =>
          Try {
            restoreTable(spark, connection, targetDatabase, tableName, ddlDir, dataDir,
              restoreStrategy, skipIfTableExists, config)
            updateRestoreProgress(progressFile, RestoreProgress(tableName, "ALL", "SUCCESS"))
            println(s"✓ Successfully restored table: $tableName")
            Thread.sleep(1000000)
          } match {
            case Failure(e) =>
              val error = RestoreError(tableName, "RESTORE_ERROR", e.getMessage)
              logError(error, errorLog)
              updateRestoreProgress(progressFile, RestoreProgress(tableName, "ERROR", e.getMessage))
              println(s"✗ Failed to restore table: $tableName, error: ${e.getMessage}")
              e.printStackTrace()
            case Success(_) => // 已处理
          }
        }
      }

      // 生成恢复报告
      generateRestoreReport(basePath, targetDatabase, config, restoreStrategy)

      println("\n" + "=" * 80)
      println("Restore Completed!")
      println("=" * 80)

    } finally {
      if (connection != null) connection.close()
    }
  }

  def restoreTable(spark: SparkSession,
                   connection: Connection,
                   targetDatabase: String,
                   tableName: String,
                   ddlDir: String,
                   dataDir: String,
                   restoreStrategy: RestoreStrategy.Value,
                   skipIfTableExists: Boolean,
                   config: com.typesafe.config.Config): Unit = {

    val progressFile = s"${config.getString("backup.basePath")}/${config.getString("restore.progressFile")}"

    try {
      println(s"Starting restore for table: $tableName")

      // 检查表是否已存在
      val tableExists = checkTableExists(connection, targetDatabase, tableName)

      if (tableExists && skipIfTableExists) {
        println(s"  Table $tableName already exists, skipping (skipIfTableExists=true)")
        updateRestoreProgress(progressFile, RestoreProgress(tableName, "SKIP", "TABLE_EXISTS"))
        return
      }

      // 1. 执行DDL创建表或处理现有表
      val ddlExecuted = restoreDDL(connection, targetDatabase, tableName, ddlDir,
        restoreStrategy, tableExists, config)

      if (ddlExecuted) {
        updateRestoreProgress(progressFile, RestoreProgress(tableName, "DDL", "SUCCESS"))
        println(s"  ✓ DDL processed for table: $tableName")
      }

      // 2. 加载Parquet数据
      restoreData(spark, connection, targetDatabase, tableName, dataDir,
        restoreStrategy, tableExists, config)

      updateRestoreProgress(progressFile, RestoreProgress(tableName, "DATA", "SUCCESS"))
      println(s"  ✓ Data restored for table: $tableName")

    } catch {
      case e: Exception =>
        val errorMsg = s"Failed to restore table $tableName: ${e.getMessage}"
        updateRestoreProgress(progressFile, RestoreProgress(tableName, "ERROR", errorMsg))
        throw new RuntimeException(errorMsg, e)
    }
  }

  def checkTableExists(connection: Connection, databaseName: String, tableName: String): Boolean = {
    val metadata = connection.getMetaData()
    val resultSet = metadata.getTables(null, databaseName, tableName, Array("TABLE"))
    val exists = resultSet.next()
    resultSet.close()
    exists
  }

  def restoreDDL(connection: Connection,
                 targetDatabase: String,
                 tableName: String,
                 ddlDir: String,
                 restoreStrategy: RestoreStrategy.Value,
                 tableExists: Boolean,
                 config: com.typesafe.config.Config): Boolean = {

    val statement = connection.createStatement()
    var ddlExecuted = false

    try {
      // 读取DDL文件
      val localDdlDir = convertToLocalPath(ddlDir)
      val ddlFile = new File(s"$localDdlDir/$tableName.sql")

      if (!ddlFile.exists()) {
        throw new RuntimeException(s"DDL file not found: ${ddlFile.getAbsolutePath}")
      }

      val reader = new BufferedReader(new FileReader(ddlFile))

      val ddl = try {
        val lines = Iterator.continually(reader.readLine()).takeWhile(_ != null).mkString("\n")
        // 移除可能的注释和末尾分号
        lines.split(";").headOption.getOrElse(lines).trim
      } finally {
        reader.close()
      }

      println(s"Processing DDL for table: $tableName in database: $targetDatabase")
      println(s"  Table exists: $tableExists")
      println(s"  Restore strategy: $restoreStrategy")

      restoreStrategy match {
        case RestoreStrategy.OVERWRITE =>
          // 覆盖模式：删除表后重建
          if (tableExists) {
            println(s"  Dropping existing table (OVERWRITE mode)")
            statement.executeUpdate(s"DROP TABLE IF EXISTS `$tableName`")
          }
          println(s"  Creating table (OVERWRITE mode)")
          statement.executeUpdate(ddl)
          ddlExecuted = true

        case RestoreStrategy.APPEND =>
          // 追加模式：如果表不存在则创建，否则跳过DDL
          if (!tableExists) {
            println(s"  Creating table (APPEND mode - table does not exist)")
            statement.executeUpdate(ddl)
            ddlExecuted = true
          } else {
            println(s"  Table already exists, skipping DDL (APPEND mode)")
            // 验证表结构兼容性（可选）
            if (!validateTableStructure(connection, targetDatabase, tableName, ddl)) {
              println(s"  WARNING: Table structure may not be compatible with backup data")
            }
          }

        case RestoreStrategy.TRUNCATE =>
          // 清空模式：如果表不存在则创建，否则清空数据
          if (!tableExists) {
            println(s"  Creating table (TRUNCATE mode - table does not exist)")
            statement.executeUpdate(ddl)
            ddlExecuted = true
          } else {
            println(s"  Table exists, truncating data (TRUNCATE mode)")
            // 先清空表数据
            statement.executeUpdate(s"TRUNCATE TABLE `$tableName`")
            // 验证表结构兼容性（可选）
            if (!validateTableStructure(connection, targetDatabase, tableName, ddl)) {
              println(s"  WARNING: Table structure may not be compatible with backup data")
            }
          }

        case _ =>
          throw new RuntimeException(s"Unsupported restore strategy: $restoreStrategy")
      }

    } finally {
      statement.close()
    }

    ddlExecuted
  }

  def validateTableStructure(connection: Connection,
                             databaseName: String,
                             tableName: String,
                             backupDDL: String): Boolean = {
    // 这里可以添加表结构验证逻辑
    // 例如：比较列名、数据类型等
    // 当前实现为简单返回true，可以根据需求扩展
    true
  }

  def restoreData(spark: SparkSession,
                  connection: Connection,
                  targetDatabase: String,
                  tableName: String,
                  dataDir: String,
                  restoreStrategy: RestoreStrategy.Value,
                  tableExists: Boolean,
                  config: com.typesafe.config.Config): Unit = {

    val targetUrl = config.getString("mysql.target.url")
    val properties = new Properties()
    properties.put("user", config.getString("mysql.target.user"))
    properties.put("password", config.getString("mysql.target.password"))
    properties.put("driver", config.getString("mysql.target.driver"))

    // 读取Parquet文件
    val tablePath = s"$dataDir/$tableName"

    try {
      val df = spark.read.parquet(tablePath)
      val rowCount = df.count()
      println(s"  Loading $rowCount rows for table: $tableName")

      // 根据恢复策略选择保存模式
      val saveMode = restoreStrategy match {
        case RestoreStrategy.OVERWRITE =>
          // 表刚被重建，使用Append实际上就是首次写入
          SaveMode.Append
        case RestoreStrategy.APPEND =>
          SaveMode.Append
        case RestoreStrategy.TRUNCATE =>
          // 表已清空，使用Append
          SaveMode.Append
        case _ =>
          SaveMode.Append
      }

      // 写入MySQL
      df.write
        .mode(saveMode)
        .option("batchsize", config.getInt("spark.jdbc.batchsize"))
        .jdbc(targetUrl, tableName, properties)

    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Failed to restore data for table $tableName: ${e.getMessage}", e)
    }
  }

  def loadRestoreProgress(progressFile: String): Set[String] = {
    val localProgressFile = convertToLocalPath(progressFile)
    val file = new File(localProgressFile)

    if (!file.exists()) {
      return Set.empty[String]
    }

    val restoredTables = mutable.Set[String]()

    try {
      val reader = new BufferedReader(new FileReader(file))
      try {
        var line: String = null
        while ({ line = reader.readLine(); line != null }) {
          val fields = line.split("\t")
          if (fields.length >= 3 && fields(2) == "SUCCESS" && fields(1) == "ALL") {
            restoredTables += fields(0)
          }
        }
      } finally {
        reader.close()
      }
    } catch {
      case e: Exception =>
        println(s"Error reading restore progress file: ${e.getMessage}")
    }

    restoredTables.toSet
  }

  def updateRestoreProgress(progressFile: String, progress: RestoreProgress): Unit = {
    val localProgressFile = convertToLocalPath(progressFile)
    val file = new File(localProgressFile)

    // 确保父目录存在
    file.getParentFile.mkdirs()

    val writer = new FileWriter(file, true)
    try {
      writer.write(s"${progress.tableName}\t${progress.step}\t${progress.status}\t${progress.timestamp}\n")
    } finally {
      writer.close()
    }
  }

  def logError(error: RestoreError, errorLog: String): Unit = {
    val localErrorLog = convertToLocalPath(errorLog)
    val file = new File(localErrorLog)

    // 确保父目录存在
    file.getParentFile.mkdirs()

    val writer = new FileWriter(file, true)
    try {
      writer.write(s"${error.timestamp}\t${error.tableName}\t${error.errorType}\t${error.errorMessage}\n")
    } finally {
      writer.close()
    }
  }

  def logError(message: String, exception: Exception, errorLog: String): Unit = {
    val localErrorLog = convertToLocalPath(errorLog)
    val file = new File(localErrorLog)

    // 确保父目录存在
    file.getParentFile.mkdirs()

    val writer = new FileWriter(file, true)
    try {
      writer.write(s"${System.currentTimeMillis()}\tGLOBAL\tFATAL_ERROR\t$message: ${exception.getMessage}\n")
      exception.printStackTrace(new PrintWriter(writer))
    } finally {
      writer.close()
    }
  }

  def convertToLocalPath(sparkPath: String): String = {
    var path = sparkPath

    // 移除file:///前缀（Windows）
    if (path.startsWith("file:///")) {
      path = path.substring(8) // 移除file:///
    }
    // 移除file://前缀（Linux/Mac）
    else if (path.startsWith("file://")) {
      path = path.substring(7) // 移除file://
    }

    // 处理Windows盘符问题
    if (System.getProperty("os.name").toLowerCase().contains("windows")) {
      // 如果路径以/开头且第二个字符是字母，可能是Windows盘符
      if (path.startsWith("/") && path.length > 2) {
        val secondChar = path.charAt(1)
        if (secondChar.isLetter) {
          // 将 /C:/ 转换为 C:/
          path = path.substring(1)
        }
      }
    }

    path
  }

  def extractDatabaseName(jdbcUrl: String): String = {
    try {
      // 解析JDBC URL格式: jdbc:mysql://host:port/database?params
      val pattern = """jdbc:mysql://[^/]+/([^?]+)""".r
      pattern.findFirstMatchIn(jdbcUrl) match {
        case Some(m) => m.group(1)
        case None =>
          // 尝试其他解析方式
          val urlWithoutParams = jdbcUrl.split("\\?")(0)
          val parts = urlWithoutParams.split("/")
          if (parts.length >= 4) {
            parts(3)
          } else {
            throw new RuntimeException(s"Could not extract database name from JDBC URL: $jdbcUrl")
          }
      }
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Could not extract database name from JDBC URL: $jdbcUrl", e)
    }
  }

  def generateRestoreReport(basePath: String, targetDatabase: String, config: com.typesafe.config.Config, restoreStrategy: RestoreStrategy.Value): Unit = {
    val localBasePath = convertToLocalPath(basePath)
    val reportFile = new File(s"$localBasePath/restore_report_${System.currentTimeMillis()}.txt")

    val writer = new PrintWriter(reportFile, "UTF-8")
    try {
      writer.println("=" * 80)
      writer.println("MySQL Database Restore Report")
      writer.println("=" * 80)
      writer.println(s"Report generated at: ${new java.util.Date()}")
      writer.println(s"Target database: $targetDatabase")
      writer.println(s"Restore strategy: $restoreStrategy")
      writer.println(s"Restore directory: $localBasePath")
      writer.println()

      // 读取进度文件
      val progressFile = new File(s"$localBasePath/${config.getString("restore.progressFile")}")
      if (progressFile.exists()) {
        val reader = new BufferedReader(new FileReader(progressFile))
        try {
          var successCount = 0
          var failedCount = 0
          var skippedCount = 0
          var existsSkippedCount = 0
          val tables = mutable.ListBuffer[String]()

          var line: String = null
          while ({ line = reader.readLine(); line != null }) {
            val fields = line.split("\t")
            if (fields.length >= 3) {
              val (tableName, step, status) = (fields(0), fields(1), fields(2))
              tables += tableName

              if (step == "ALL" && status == "SUCCESS") {
                successCount += 1
              } else if (status == "FAILED" || status.startsWith("ERROR")) {
                failedCount += 1
              } else if (step == "SKIP" && status == "SKIPPED") {
                skippedCount += 1
              } else if (step == "SKIP" && status == "TABLE_EXISTS") {
                existsSkippedCount += 1
              }
            }
          }

          writer.println(s"Total tables processed: ${tables.size}")
          writer.println(s"Successfully restored: $successCount")
          writer.println(s"Failed to restore: $failedCount")
          writer.println(s"Skipped (by pattern): $skippedCount")
          writer.println(s"Skipped (table exists): $existsSkippedCount")
          writer.println()

          if (failedCount > 0) {
            writer.println("Failed tables:")
            // 重新读取文件获取失败的表
            val failedReader = new BufferedReader(new FileReader(progressFile))
            try {
              var failedLine: String = null
              while ({ failedLine = failedReader.readLine(); failedLine != null }) {
                val fields = failedLine.split("\t")
                if (fields.length >= 3 && (fields(2) == "FAILED" || fields(2).startsWith("ERROR"))) {
                  writer.println(s"  - ${fields(0)}: ${fields(2)}")
                }
              }
            } finally {
              failedReader.close()
            }
            writer.println()
          }

        } finally {
          reader.close()
        }
      }

      writer.println("=" * 80)
      writer.println("Restore completed!")

    } finally {
      writer.close()
    }

    println(s"\nRestore report generated: ${reportFile.getAbsolutePath}")
  }
}