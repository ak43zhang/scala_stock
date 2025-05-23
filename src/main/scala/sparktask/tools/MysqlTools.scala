package sparktask.tools

import java.sql.DriverManager

object MysqlTools {

  /**
   * mysql执行器
   */
  def mysqlEx(tableName:String,query:String): Unit ={
    // 配置 MySQL 连接信息
    val jdbcUrl = "jdbc:mysql://localhost:3306/gs"
    val username = "root"
    val password = "123456"

    var connection: java.sql.Connection = null
    var statement: java.sql.Statement = null
    try {
      // 建立 JDBC 连接
      connection = DriverManager.getConnection(jdbcUrl, username, password)
      // 创建 Statement 对象
      statement = connection.createStatement()

      // 执行删除操作
      val rowsAffected = statement.executeUpdate(query)
      println(s"成功删除 $rowsAffected 行数据。")
    } catch {
      case e: Exception =>
        println(s"删除数据时出错: ${e.getMessage}")
    } finally {
      if (statement != null) statement.close()
      if (connection != null) connection.close()
    }
  }
}
