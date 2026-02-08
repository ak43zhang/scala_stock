package spark.tools

import java.util.Properties

/**
 * mysql配置文件工具类
 */
object MysqlProperties {

  def getMysqlProperties(): Properties ={
    val url = "jdbc:mysql://192.168.0.100:3306/gs"
    val driver = "com.mysql.cj.jdbc.Driver"
    val user = "root"
    val pwd = "123456"

    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", pwd)
    properties.setProperty("url", url)
    properties.setProperty("driver", driver)
//    properties.setProperty("partitionColumn", "amount") // 选择一个分区列
//    properties.setProperty("lowerBound", "10000") // 分区列的最小值
//    properties.setProperty("upperBound", "1000000") // 分区列的最大值
//    properties.setProperty("numPartitions", "8") // 设置分区数量
    // 关键：设置批量写入参数
    properties.setProperty("rewriteBatchedStatements", "true")
    properties.setProperty("useCompression", "true")
    properties.setProperty("cachePrepStmts", "true")
    properties.setProperty("prepStmtCacheSize", "250")
    properties.setProperty("prepStmtCacheSqlLimit", "2048")
    properties.setProperty("useServerPrepStmts", "true")

    properties
  }

}
