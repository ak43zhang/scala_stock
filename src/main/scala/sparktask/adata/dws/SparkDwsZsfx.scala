package sparktask.adata.dws

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 指数分析
 */
object SparkDwsZsfx {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "10")
      .set("spark.driver.memory", "8g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "10")
      .set("spark.local.dir", "D:\\SparkTemp")

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val startm = System.currentTimeMillis()
    val df = spark.read.parquet(s"file:///D:\\gsdata\\gpsj_hs_10days\\trade_date_month=2024*",s"file:///D:\\gsdata\\gpsj_hs_10days\\trade_date_month=2025*")
    df.cache()
    df.createTempView("ta1")

    spark.sql(
      """select * from ta1
        |where t0_sfzt=0 and t1_sfzt=0 and t0_cjzt=0 and t1_cjzt=0
        |and t0_spzf<=2 and t1_spzf<=2
        |and stock_code in (select stock_code from ta1 where t2_trade_date>'2024-11-01' and t2_trade_date<'2025-01-07' and t2_sfzt=1)
        |and t2_zgzf>8 and t2_spzf-t2_zgzf<0 and t2_trade_date='2025-01-08'""".stripMargin).show(200)


    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }

  /**
   * 上证指数，
   */
  def fx1(): Unit ={

  }

  def fx2(): Unit ={

  }

  def fx3(): Unit ={

  }

  /**
   * 股性分析
    */
  def gxfx(): Unit ={


  }

}
