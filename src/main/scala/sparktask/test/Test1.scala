package sparktask.test

import java.io.File
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

object Test1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "10")
      .set("spark.sql.broadcastTimeout","60000")
      .set("spark.driver.memory", "6g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "10")
      .set("spark.local.dir", "D:\\SparkTemp")

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

    val g8_table = "g8_data_temp"
    val g8_table_2 = "g8_data_2"

    //涨停板dataframe
    val g8_df: DataFrame = spark.read.jdbc(url, g8_table, properties)
    g8_df.createOrReplaceTempView("t1")

    val columnsList = ArrayBuffer[String]("stock_code","t0_trade_date","t1_trade_date","t2_trade_date","t3_trade_date",
      "t0_close","t0_sfzt","t0_cjzt","t0_kxzt","t0_ln","t0_zrlnb","t0_qjzf","t0_stzf","t0_kpzf","t0_zgzf","t0_zdzf","t0_spzf",
      "t1_close","t1_sfzt","t1_cjzt","t1_kxzt","t1_ln","t1_zrlnb","t1_qjzf","t1_stzf","t1_kpzf","t1_zgzf","t1_zdzf","t1_spzf",
      "t2_close","t2_sfzt","t2_cjzt","t2_kxzt","t2_ln","t2_zrlnb","t2_qjzf","t2_stzf","t2_kpzf","t2_zgzf","t2_zdzf","t2_spzf"
    )
    val data20_df = spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_10days\\trade_date_month=20[15,16,17,18,19,20,21,22,23,24,25]*")
      .select(columnsList.map(col): _*)
    data20_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    data20_df.createOrReplaceTempView("t2")

    spark.sql("select * from t2 where stock_code='603335' and t1_trade_date = '2025-09-26'").show()

    spark.sql(
      """
        |select t1.*,
        |t2.t0_kpzf,t2.t0_zgzf,t2.t0_zdzf,t2.t0_spzf,t2.t0_qjzf,t2.t0_stzf,
        |t2.t1_stzf,t2.t1_qjzf,t2.t1_zdzf,
        |t2.t2_stzf,t2.t2_qjzf,t2.t2_zdzf
        |from
        |t1 left join t2 on t1.`代码`=t2.stock_code and t1.buy_date = t2.t1_trade_date
        |order by buy_date desc
        |""".stripMargin)
      .select("代码","trade_time","buy_date","high_low_pressure","high_low_support","channel_position","support_ratio","pressure_ratio","sx","fxdx_lk","s","zdf"
        ,"t0_kpzf","t0_zgzf","t0_zdzf","t0_spzf","t0_qjzf","t0_stzf"
        ,"t1_sfzt","t1_cjzt","t1_kpzf","t1_zgzf","t1_zdzf","t1_spzf","t1_qjzf","t1_stzf",
        "t2_kpzf","t2_zgzf","t2_zdzf","t2_spzf","t2_qjzf","t2_stzf")
      .distinct().write.mode("append").jdbc(url, g8_table_2, properties)

    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
  }


}
