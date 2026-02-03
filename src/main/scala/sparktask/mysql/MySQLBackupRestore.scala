package sparktask.mysql

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import java.sql.{Connection, DriverManager, Statement, ResultSet, DatabaseMetaData}
import java.util.Properties
import scala.collection.mutable.{ArrayBuffer, HashMap}
import java.io.{File, FileWriter, PrintWriter}
import org.apache.spark.sql.types.StructType

object MySQLBackupRestore {

  // 配置参数
  case class Config(
                     mysqlUrl: String = "jdbc:mysql://192.168.0.109:3306",
                     mysqlUser: String = "root",
                     mysqlPassword: String = "123456",
                     databaseName: String = "gs",
                     parquetPath_spark: String = "file:///H:/mysql_backup/parquet",
                     parquetPath: String = "H:/mysql_backup/parquet",
                     ddlBackupPath: String = "file:///H:/mysql_backup/ddl",
                     newMysqlUrl: String = "jdbc:mysql://192.168.0.109:3306",
                     newMysqlUser: String = "root",
                     newMysqlPassword: String = "123456"
                   )

  def main(args: Array[String]): Unit = {
    val config = Config() // 可根据需要从命令行参数解析

    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("MySQL Backup and Restore")
      .master("local[*]") // 生产环境根据实际情况调整
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .getOrCreate()

    try {
      // 步骤1: 备份数据为Parquet文件
      backupDatabaseToParquet(spark, config)

      // 步骤2: 获取所有表的DDL
      val tableDDLs = backupTableDDLs(config)

      // 步骤3: 保存DDL到文件
      saveDDLsToFile(tableDDLs, s"${config.ddlBackupPath}/table_ddls.sql")

      // 步骤4: 初始化MySQL后恢复表结构并导入数据
//      restoreDatabase(spark, config, tableDDLs)

      println("数据库备份与恢复流程完成!")

    } catch {
      case e: Exception =>
        println(s"处理过程中发生错误: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * 步骤1: 将MySQL数据库所有表备份为Parquet文件
   */
  def backupDatabaseToParquet(spark: SparkSession, config: Config): Unit = {
    println("开始备份数据库到Parquet文件...")

    // 创建备份目录
    val parquetDir = new File(config.parquetPath_spark)
    if (!parquetDir.exists()) parquetDir.mkdirs()

    // 获取数据库中的所有表
    val tables = getTableList(config)

    println(s"数据库 ${config.databaseName} 中共有 ${tables.size} 张表")

    // MySQL连接属性
    val connectionProperties = new Properties()
    connectionProperties.put("user", config.mysqlUser)
    connectionProperties.put("password", config.mysqlPassword)
    connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver")

    // 为每张表读取数据并保存为Parquet
    tables.foreach { tableName =>
      println(s"处理表: $tableName")

      try {
        // 读取MySQL表数据
        val jdbcUrl = s"${config.mysqlUrl}/${config.databaseName}"
        val df = spark.read
          .jdbc(jdbcUrl, tableName, connectionProperties)

        // 保存为Parquet文件，按表名创建子目录
        val tableParquetPath = s"${config.parquetPath_spark}/${config.databaseName}/$tableName"

        // 检查是否有日期/时间分区列，如果有则进行分区保存
        val dateColumns = df.schema.fields.filter(f =>
          f.dataType.typeName.contains("Date") ||
            f.dataType.typeName.contains("Timestamp")
        ).map(_.name)

        if (dateColumns.nonEmpty && df.count() > 1000000) {
          // 大数据量表，使用日期分区
          val partitionColumn = dateColumns.head
          df.write
            .mode(SaveMode.Overwrite)
            .partitionBy(partitionColumn)
            .parquet(tableParquetPath)
          println(s"  -> 表 $tableName 已分区保存到 $tableParquetPath (分区列: $partitionColumn)")
        } else {
          // 小数据量表，直接保存
          df.write
            .mode(SaveMode.Overwrite)
            .parquet(tableParquetPath)
          println(s"  -> 表 $tableName 已保存到 $tableParquetPath")
        }

        // 保存表结构信息（可选，用于验证）
        saveSchemaInfo(df.schema, tableName, config)

      } catch {
        case e: Exception =>
          println(s"  -> 处理表 $tableName 时出错: ${e.getMessage}")
        // 记录错误但继续处理其他表
      }
    }

    println("数据库备份完成!")
  }

  /**
   * 获取数据库中的所有表名
   */
  def getTableList(config: Config): Array[String] = {
    var connection: Connection = null
    var resultSet: ResultSet = null

    try {
      Class.forName("com.mysql.cj.jdbc.Driver")
      val jdbcUrl = s"${config.mysqlUrl}/${config.databaseName}"
      connection = DriverManager.getConnection(jdbcUrl, config.mysqlUser, config.mysqlPassword)

      val metaData = connection.getMetaData()
      resultSet = metaData.getTables(config.databaseName, null, "%", Array("TABLE"))

      val tables = ArrayBuffer[String]()
      while (resultSet.next()) {
        val tableName = resultSet.getString("TABLE_NAME")
        // 排除系统表
        if (!tableName.startsWith("sys_") && !tableName.startsWith("mysql_")) {
          tables += tableName
        }
      }

      tables.toArray

    } catch {
      case e: Exception =>
        println(s"获取表列表失败: ${e.getMessage}")
        Array.empty[String]
    } finally {
      if (resultSet != null) resultSet.close()
      if (connection != null) connection.close()
    }
  }

  /**
   * 保存表结构信息（用于验证）
   */
  def saveSchemaInfo(schema: StructType, tableName: String, config: Config): Unit = {
    val schemaFile = s"${config.parquetPath}/${config.databaseName}/${tableName}_schema.json"
    val schemaJson = schema.prettyJson

    val writer = new PrintWriter(new File(schemaFile))
    writer.write(schemaJson)
    writer.close()
  }

  /**
   * 步骤2: 备份所有表的DDL语句
   */
  def backupTableDDLs(config: Config): Map[String, String] = {
    println("开始备份表结构DDL...")

    var connection: Connection = null
    var statement: Statement = null
    var resultSet: ResultSet = null

    val tableDDLs = HashMap[String, String]()

    try {
      Class.forName("com.mysql.cj.jdbc.Driver")
      val jdbcUrl = s"${config.mysqlUrl}/${config.databaseName}"
      connection = DriverManager.getConnection(jdbcUrl, config.mysqlUser, config.mysqlPassword)
      statement = connection.createStatement()

      // 获取所有表名
      val tables = getTableList(config)

      tables.foreach { tableName =>
        try {
          // 执行SHOW CREATE TABLE获取DDL
          resultSet = statement.executeQuery(s"SHOW CREATE TABLE `$tableName`")

          if (resultSet.next()) {
            val ddl = resultSet.getString(2) // 第二列是CREATE TABLE语句
            tableDDLs.put(tableName, ddl)
            println(s"  -> 表 $tableName 的DDL已获取")
          }

        } catch {
          case e: Exception =>
            println(s"  -> 获取表 $tableName 的DDL失败: ${e.getMessage}")
        } finally {
          if (resultSet != null) resultSet.close()
        }
      }

      // 获取存储过程、函数、触发器等（可选）
      backupRoutinesAndTriggers(connection, config.databaseName, config.ddlBackupPath)

    } catch {
      case e: Exception =>
        println(s"备份DDL失败: ${e.getMessage}")
    } finally {
      if (statement != null) statement.close()
      if (connection != null) connection.close()
    }

    println(s"共获取 ${tableDDLs.size} 张表的DDL")
    tableDDLs.toMap
  }

  /**
   * 备份存储过程和触发器（可选）
   */
  def backupRoutinesAndTriggers(connection: Connection, databaseName: String, backupPath: String): Unit = {
    try {
      val statement = connection.createStatement()

      // 备份存储过程
      val routinesResult = statement.executeQuery(
        s"SELECT ROUTINE_NAME, ROUTINE_DEFINITION FROM INFORMATION_SCHEMA.ROUTINES " +
          s"WHERE ROUTINE_SCHEMA = '$databaseName' AND ROUTINE_TYPE = 'PROCEDURE'"
      )

      val routinesFile = s"$backupPath/routines.sql"
      val routinesWriter = new PrintWriter(new File(routinesFile))

      while (routinesResult.next()) {
        val routineName = routinesResult.getString("ROUTINE_NAME")
        val routineDef = routinesResult.getString("ROUTINE_DEFINITION")
        routinesWriter.println(s"DELIMITER $$")
        routinesWriter.println(s"CREATE PROCEDURE `$routineName`")
        routinesWriter.println(routineDef)
        routinesWriter.println(s"$$")
        routinesWriter.println(s"DELIMITER ;")
        routinesWriter.println()
      }

      routinesWriter.close()
      routinesResult.close()

      // 备份触发器
      val triggersResult = statement.executeQuery(
        s"SELECT TRIGGER_NAME, ACTION_STATEMENT FROM INFORMATION_SCHEMA.TRIGGERS " +
          s"WHERE TRIGGER_SCHEMA = '$databaseName'"
      )

      val triggersFile = s"$backupPath/triggers.sql"
      val triggersWriter = new PrintWriter(new File(triggersFile))

      while (triggersResult.next()) {
        val triggerName = triggersResult.getString("TRIGGER_NAME")
        val triggerDef = triggersResult.getString("ACTION_STATEMENT")
        triggersWriter.println(s"CREATE TRIGGER `$triggerName`")
        triggersWriter.println(triggerDef)
        triggersWriter.println()
      }

      triggersWriter.close()
      triggersResult.close()
      statement.close()

      println("存储过程和触发器已备份")

    } catch {
      case e: Exception =>
        println(s"备份存储过程和触发器失败: ${e.getMessage}")
    }
  }

  /**
   * 步骤3: 保存DDL到文件
   */
  def saveDDLsToFile(tableDDLs: Map[String, String], filePath: String): Unit = {
    val file = new File(filePath)
    file.getParentFile.mkdirs()

    val writer = new PrintWriter(new File(filePath))

    // 写入数据库创建语句
    writer.println("-- MySQL数据库备份DDL文件")
    writer.println(s"-- 生成时间: ${new java.util.Date()}")
    writer.println(s"CREATE DATABASE IF NOT EXISTS `a` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
    writer.println(s"USE `a`;")
    writer.println()

    // 按表名排序写入DDL
    tableDDLs.toSeq.sortBy(_._1).foreach { case (tableName, ddl) =>
      writer.println(ddl + ";")
      writer.println()
    }

    writer.close()
    println(s"DDL已保存到: $filePath")
  }

  /**
   * 步骤4: 恢复数据库（MySQL初始化后调用）
   */
  def restoreDatabase(spark: SparkSession, config: Config, tableDDLs: Map[String, String]): Unit = {
    println("开始恢复数据库...")

    // 创建新数据库连接属性
    val newConnectionProperties = new Properties()
    newConnectionProperties.put("user", config.newMysqlUser)
    newConnectionProperties.put("password", config.newMysqlPassword)
    newConnectionProperties.put("driver", "com.mysql.cj.jdbc.Driver")
    newConnectionProperties.put("rewriteBatchedStatements", "true")

    var connection: Connection = null
    var statement: Statement = null

    try {
      // 连接新MySQL实例（假设已经初始化）
      Class.forName("com.mysql.cj.jdbc.Driver")
      connection = DriverManager.getConnection(config.newMysqlUrl, config.newMysqlUser, config.newMysqlPassword)
      statement = connection.createStatement()

      // 创建数据库（如果不存在）
      statement.execute(s"CREATE DATABASE IF NOT EXISTS `${config.databaseName}` " +
        s"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
      statement.execute(s"USE `${config.databaseName}`")

      println(s"数据库 ${config.databaseName} 已创建/准备就绪")

      // 按顺序创建表（处理外键依赖）
      val sortedTables = sortTablesByDependency(tableDDLs)

      sortedTables.foreach { tableName =>
        val ddl = tableDDLs.getOrElse(tableName, "")
        if (ddl.nonEmpty) {
          try {
            // 移除已有的表（如果存在）
            statement.execute(s"DROP TABLE IF EXISTS `$tableName`")

            // 执行CREATE TABLE语句
            statement.execute(ddl)
            println(s"  -> 表 $tableName 创建成功")

            // 从Parquet文件恢复数据
            restoreTableData(spark, tableName, config, newConnectionProperties)

          } catch {
            case e: Exception =>
              println(s"  -> 创建表 $tableName 失败: ${e.getMessage}")
              // 记录详细错误
              println(s"DDL语句: ${ddl.take(100)}...")
          }
        }
      }

      // 恢复存储过程和触发器（可选）
      restoreRoutinesAndTriggers(connection, config.ddlBackupPath)

      println("数据库恢复完成!")

    } catch {
      case e: Exception =>
        println(s"恢复数据库失败: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      if (statement != null) statement.close()
      if (connection != null) connection.close()
    }
  }

  /**
   * 根据外键依赖对表进行排序
   */
  def sortTablesByDependency(tableDDLs: Map[String, String]): List[String] = {
    // 简化的依赖排序（实际生产中可能需要更复杂的依赖分析）
    // 这里假设没有外键依赖或依赖关系简单
    tableDDLs.keys.toList.sorted
  }

  /**
   * 从Parquet文件恢复表数据
   */
  def restoreTableData(spark: SparkSession, tableName: String, config: Config,
                       connectionProperties: Properties): Unit = {
    val parquetPath = s"${config.parquetPath_spark}/${config.databaseName}/$tableName"
    val parquetDir = new File(parquetPath)

    if (parquetDir.exists() && parquetDir.list().nonEmpty) {
      println(s"    -> 正在恢复表 $tableName 的数据...")

      try {
        // 读取Parquet文件
        val df = spark.read.parquet(parquetPath)

        // 写入MySQL
        val jdbcUrl = s"${config.newMysqlUrl}/${config.databaseName}"

        // 配置写入参数
        val writeProperties = new Properties()
        writeProperties.put("user", config.newMysqlUser)
        writeProperties.put("password", config.newMysqlPassword)
        writeProperties.put("driver", "com.mysql.cj.jdbc.Driver")
        writeProperties.put("batchsize", "50000") // 批处理大小
        writeProperties.put("isolationLevel", "NONE") // 提高写入性能

        // 写入数据
        df.write
          .mode(SaveMode.Append)
          .option("truncate", "true") // 如果表已存在数据，先清空
          .jdbc(jdbcUrl, tableName, writeProperties)

        val rowCount = df.count()
        println(s"    -> 表 $tableName 数据恢复完成，共 $rowCount 行")

      } catch {
        case e: Exception =>
          println(s"    -> 恢复表 $tableName 数据失败: ${e.getMessage}")
      }
    } else {
      println(s"    -> 表 $tableName 的Parquet文件不存在或为空，跳过数据恢复")
    }
  }

  /**
   * 恢复存储过程和触发器
   */
  def restoreRoutinesAndTriggers(connection: Connection, backupPath: String): Unit = {
    val routinesFile = s"$backupPath/routines.sql"
    val triggersFile = s"$backupPath/triggers.sql"

    // 恢复存储过程
    if (new File(routinesFile).exists()) {
      println("恢复存储过程...")
      val routinesSql = scala.io.Source.fromFile(routinesFile).mkString
      val statements = routinesSql.split("DELIMITER \\$\\$")

      statements.foreach { stmt =>
        if (stmt.trim.nonEmpty) {
          try {
            connection.createStatement().execute(stmt)
          } catch {
            case e: Exception => println(s"执行存储过程语句失败: ${e.getMessage}")
          }
        }
      }
    }

    // 恢复触发器
    if (new File(triggersFile).exists()) {
      println("恢复触发器...")
      val triggersSql = scala.io.Source.fromFile(triggersFile).mkString
      val statements = triggersSql.split(";")

      statements.foreach { stmt =>
        if (stmt.trim.nonEmpty) {
          try {
            connection.createStatement().execute(stmt)
          } catch {
            case e: Exception => println(s"执行触发器语句失败: ${e.getMessage}")
          }
        }
      }
    }
  }
}