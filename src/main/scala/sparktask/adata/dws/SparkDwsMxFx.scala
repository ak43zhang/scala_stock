package sparktask.adata.dws

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 明细数据分析
 *
 *
 *
 */
object SparkDwsMxFx {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions","10")
      .set("spark.driver.memory","8g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "10")
      .set("spark.local.dir","D:\\SparkTemp")

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val url = "jdbc:mysql://localhost:3306/gs"
    val driver = "com.mysql.cj.jdbc.Driver"
    val user = "root"
    val pwd = "123456"

    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", pwd)
    properties.setProperty("url", url)
    properties.setProperty("driver", driver)

    val startm = System.currentTimeMillis()

    var df:DataFrame = spark.read.jdbc(url, "data_ss_fshq_20250108", properties)

    df.createTempView("ta1")
    spark.sql(
      """
        |select *,sum(change_pct) over(partition by stock_code ORDER BY stock_code,row ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) as `pre_change_pct` ,
        |change_pct-sum(change_pct) over(partition by stock_code ORDER BY stock_code,row ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) as `1分钟涨速`,
        |if(change_pct-sum(change_pct) over(partition by stock_code ORDER BY stock_code,row ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)>0 and trade_time='2025-01-08 09:31' ,1,0) as bj
        |from
        |(select *,row_number() over(partition by stock_code order by trade_time,stock_code) as row from ta1) """.stripMargin)
      .where("`1分钟涨速`>2 and trade_time>'2025-01-08 09:30' and bj=1").show(100)


    val endm = System.currentTimeMillis()
    println("共耗时："+(endm-startm)/1000+"秒")
    spark.close()
}
}
