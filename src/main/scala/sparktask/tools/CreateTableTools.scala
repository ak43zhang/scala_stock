package sparktask.tools

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.HashMap

/**
 * 创建表工具类
 */
object CreateTableTools {
  /**
   * 通过mysql创建临时表
   * key:mysql表名
   * value:临时表名
   * @param spark
   * @param tables
   */
  def createTableView2Mysql(spark:SparkSession,tables:HashMap[String,String]): Unit ={
    val url = "jdbc:mysql://localhost:3306/gs"
    val driver = "com.mysql.cj.jdbc.Driver"
    val user = "root"
    val pwd = "123456"

    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", pwd)
    properties.setProperty("url", url)
    properties.setProperty("driver", driver)

    tables.foreach(t=>{
      val df:DataFrame = spark.read.jdbc(url, t._1, properties)
      df.createOrReplaceTempView(t._2)
    })
  }

  /**
   * 通过parquet创建临时表
   * key:路径
   * value：临时表名
   * @param spark
   * @param tables
   */
  def createTableView2Parquet(spark:SparkSession,tables:HashMap[String,String]): Unit ={
    tables.foreach(t=>{
      val df:DataFrame = spark.read.parquet(t._1)
      df.createOrReplaceTempView(t._2)
    })
  }
}
