package spark.model

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import spark.ParameterSet
import spark.analysis.SparkAnalysisZtb3.{getPreviousDay, getYearStr}
import spark.tools.MysqlProperties
import sparktask.tools.MysqlTools

import scala.collection.mutable.ArrayBuffer

/**
 * 连板断板反抽模型
 * 近10个交易日
 *    有三个以上涨停
 *    趋势线段小于2（一不做，二不休。三舍弃[三而竭]）
 * 上一个交易日最高涨幅小于2【弱】
 * 买当日涨停票【转强】
 *
 */
object Model_ConnectDrawBack {

  val start_time ="2026-02-10"
//  val start_time ="2025-12-25"
  val end_time ="2026-02-10"

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

    val url = "jdbc:mysql://localhost:3306/gs"
    val properties = MysqlProperties.getMysqlProperties()
    val startm = System.currentTimeMillis()

    create_table(spark:SparkSession,properties: Properties)

    val jyrls = spark.read.jdbc(url, "data_jyrl", properties)
      .where(s"trade_status=1 and trade_date between '$start_time' and '$end_time'")
      .orderBy(col("trade_date").desc)
      .select("trade_date").collect().map(f => f.getAs[String]("trade_date")).toList

    for (jyrl <- jyrls) {
      val startm = System.currentTimeMillis()

      val setdate = jyrl
      val date_list = spark.read.jdbc(url, "data_jyrl", properties).orderBy(col("trade_date").desc)
        .where(s"trade_status=1 and trade_date<'$jyrl'").collect().map(f => f.getAs[String]("trade_date"))
      //yes_day 设置时间的前一个交易日
      val yes_day = date_list.toList(0)
      val n_day_2 = date_list.toList(1)
      val n_day_10 = date_list.toList(10)
      //      println(n_day_ago)
      val year = setdate.substring(0, 4)
      println(s"==========================设置时间为${setdate},则处理的是${setdate}的股票，分析股票则用上一交易日：${yes_day},${n_day_10}到${n_day_2} 10个交易日============================")

      val sql =
        s"""
           |select a.`股票代码`,'${setdate}' as trade_date,'${yes_day}' as yes_date from
           |   (select count(1) as c,sum(if(`几天几板`='首板涨停',1,0)) as qs,`股票代码` from ztb_day where trade_date between '${n_day_10}' and '${n_day_2}'  and `股票简称` not like '%ST%' group by `股票代码`  having c>=3 and qs<2 ) as a
           |where not exists (select `股票代码` from ztb_day as b where trade_date ='${yes_day}' and a.`股票代码`= b.`股票代码` and `股票简称` not like '%ST%')
           |""".stripMargin
      println(sql)
      val df = spark.sql(sql)
      df.createOrReplaceTempView("z10")

//      df.show(100)

      val res_df = spark.sql(
        """
          |select * from z10 left join data20_df on z10.`股票代码` = data20_df.`stock_code` and z10.yes_date = data20_df.t7_trade_date
          |where stock_code is not null
          |""".stripMargin)
        .orderBy(col("t7_zgzf").desc).drop("yes_date")

//      res_df.select("t7_zgzf","t7_zdzf","t7_kpzf","t7_spzf").show(100)

      res_df.where("t7_zgzf<2").show(100)

      //刪除並更新g8（大于8个点的数据分析）
      val table_name = "model_connect_draw_back"
      // 执行删除操作
      try {

        // 通过 Spark 执行 SQL 删除语句
        // 编写 SQL 删除语句
        val deleteQuery = s"DELETE FROM $table_name WHERE t8_trade_date='$setdate'"
        MysqlTools.mysqlEx(deleteQuery)
        println("数据删除成功！")
      } catch {
        case e: Exception => println(s"数据删除失败: ${e.getMessage}")
      }

      res_df.where("t8_sfzt=1 or t8_cjzt=1")
        .distinct().write.mode("append")
        .jdbc(url, table_name, properties)


    }



    val endm = System.currentTimeMillis()
    println("共耗时："+(endm-startm)/1000+"秒")
    spark.close()
  }

  def create_table(spark:SparkSession,properties: Properties): Unit = {
    val url = properties.getProperty("url")

    val jyrldf: DataFrame = spark.read.jdbc(url, "data_jyrl", properties)
    jyrldf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    jyrldf.createOrReplaceTempView("data_jyrl")

    val ztbdf: DataFrame = spark.read.jdbc(url, s"ztb_day", properties)
      .select("trade_date","`股票代码`","`股票简称`","`几天几板`","analysis").distinct()
    ztbdf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    ztbdf.createOrReplaceTempView("ztb_day")

    val columnsList = ArrayBuffer[String]("stock_code","t6_trade_date","t7_trade_date","t8_trade_date","t9_trade_date",
      "t0_close","t0_sfzt","t0_cjzt","t0_kxzt","t0_ln","t0_zrlnb","t0_qjzf","t0_stzf","t0_kpzf","t0_zgzf","t0_zdzf","t0_spzf",
      "t1_close","t1_sfzt","t1_cjzt","t1_kxzt","t1_ln","t1_zrlnb","t1_qjzf","t1_stzf","t1_kpzf","t1_zgzf","t1_zdzf","t1_spzf",
      "t2_close","t2_sfzt","t2_cjzt","t2_kxzt","t2_ln","t2_zrlnb","t2_qjzf","t2_stzf","t2_kpzf","t2_zgzf","t2_zdzf","t2_spzf",
      "t3_close","t3_sfzt","t3_cjzt","t3_kxzt","t3_ln","t3_zrlnb","t3_qjzf","t3_stzf","t3_kpzf","t3_zgzf","t3_zdzf","t3_spzf",
      "t4_close","t4_sfzt","t4_cjzt","t4_kxzt","t4_ln","t4_zrlnb","t4_qjzf","t4_stzf","t4_kpzf","t4_zgzf","t4_zdzf","t4_spzf",
      "t5_close","t5_sfzt","t5_cjzt","t5_kxzt","t5_ln","t5_zrlnb","t5_qjzf","t5_stzf","t5_kpzf","t5_zgzf","t5_zdzf","t5_spzf",
      "t6_close","t6_sfzt","t6_cjzt","t6_kxzt","t6_ln","t6_zrlnb","t6_qjzf","t6_stzf","t6_kpzf","t6_zgzf","t6_zdzf","t6_spzf",
      "t7_close","t7_sfzt","t7_cjzt","t7_kxzt","t7_ln","t7_zrlnb","t7_qjzf","t7_stzf","t7_kpzf","t7_zgzf","t7_zdzf","t7_spzf",
      "t8_close","t8_sfzt","t8_cjzt","t8_kxzt","t8_ln","t8_zrlnb","t8_qjzf","t8_stzf","t8_kpzf","t8_zgzf","t8_zdzf","t8_spzf",
      "t9_close","t9_sfzt","t9_cjzt","t9_kxzt","t9_ln","t9_zrlnb","t9_qjzf","t9_stzf","t9_kpzf","t9_zgzf","t9_zdzf","t9_spzf"
    )
    val data20_df = spark.read.parquet(s"file:///D:\\${ParameterSet.data_content}\\gpsj_hs_10days\\trade_date_month=20[${getYearStr(start_time,end_time)}]*")
      .select(columnsList.map(col): _*)
    data20_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    data20_df.createOrReplaceTempView("data20_df")

  }
}
