package spark.analysis

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import sparktask.tools.MysqlTools

/**
 * 结果推测
 */
object SparkAnalysisResultInfer {

  val table_name = "g8_data"
  val column = "result2"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.rpc.askTimeout","600")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "10")
      .set("spark.driver.memory", "10g")
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

    create_table(spark:SparkSession,url,properties)

    val startm = System.currentTimeMillis()

    /**
     * 分析子表 child_table 简称ct
     * 明细表数据  detail_df
     * 胜率表数据  winning_rate_df
     * 结果表数据  result_df
     */

    val start_time ="2025-01-01"
    val end_time ="2025-10-27"

    val jyrls = spark.read.jdbc(url, "data_jyrl", properties)
      .where(s"trade_status=1 and trade_date between '$start_time' and '$end_time'")
      .orderBy(col("trade_date").desc)
      .select("trade_date").collect().map(f => f.getAs[String]("trade_date")
      .substring(0,7)
    ).distinct
      .toList

    var assemble_df:DataFrame = null
    var detail_assemble_df:DataFrame = null
    for (jyrl <- jyrls) {
      println("============================当前交易月份为"+jyrl)
      /**
       * 参数
       *
       *   BETWEEN '2016-01-01' and '2026-01-01'
       *    `首次涨停时间`>'09:40:00' and
       *    and support_ratio*pressure_ratio<13000
       *
       *    and t0_stzf<4
       *    and zdf<6
       */
      spark.sql(
        s"""
          |select t1.*,t2.`股票简称`,t2.`首次涨停时间`,
          |round((1+(t1_spzf-t1_zgzf+(t2_kpzf+t2_zgzf+t2_spzf)/3)/100-0.0017),2) as result,
          |round(if(t2_zgzf<0,1+(t1_spzf-t1_zgzf+t2_spzf)/100-0.0017,1+(t1_spzf-t1_zgzf+(t2_kpzf+t2_zgzf+t2_spzf)/3)/100-0.0017),2) as result2,
          |   row_number() over(partition by buy_date order by `首次涨停时间`) as row_NUM
          |from
          |   (select * from `${table_name}`
          |   where t1_kpzf between -4 and 4
          |   and buy_date like '%${jyrl}%'
          |   and t0_stzf<2
          |   and  (t1_sfzt=1 or t1_cjzt=1)) as t1
          |   left join
          |   wencaiquery_zt_zb as t2
          |   on t1.`代码`=t2.`股票代码` and t1.buy_date=t2.trade_date
          |where `首次涨停时间`>'09:35:00' and `首次涨停时间`<'11:30:00'

          |order by buy_date
          |""".stripMargin)
        .where(s"row_num=1 AND $column<=1.11")
        .createOrReplaceTempView("ct")
      /**
       *  按月分析
       */
      val detail_df = spark.sql(
        """
          |select * from ct
          |order by buy_date
          |""".stripMargin)

      detail_df.show()

      if(detail_assemble_df==null){
        detail_assemble_df = detail_df
      }else{
        detail_assemble_df = detail_assemble_df.union(detail_df)
      }


      //

      val winning_rate_df = spark.sql(
        s"""
          |select count(1),
          |  sum(t1_sfzt) as zt_count,
          |  sum(t1_cjzt) as zb_count,
          |  sum(if($column>1,1,0)) as win_count,
          |  round(sum(if($column>1,1,0))/count(1),2) as win_rate,
          |  sum(if($column<=1,1,0)) as false_count,
          |  round(sum(if($column<=1,1,0))/count(1),2) as false_rate
          |from ct
          |""".stripMargin)

      winning_rate_df.show()

      val result_df = spark.sql(
        s"""
          |select round(EXP(SUM(LOG($column))),2) as $column,'$jyrl' as m from ct
          |""".stripMargin)

      result_df.show()

      if(assemble_df==null){
        assemble_df = result_df
      }else{
        assemble_df = assemble_df.union(result_df)
      }

    }

    detail_assemble_df.createOrReplaceTempView("detail_assemble_df")
    println("================================================================detail_assemble_df")
    spark.sql(
      """select
        |`代码`,buy_date,channel_position,support_ratio,pressure_ratio,sx,is_fxdx_lk,s,zdf,t0_stzf,t1_sfzt,t1_cjzt,
        |t1_kpzf,t1_zgzf,t1_spzf,t2_kpzf,t2_zgzf,t2_spzf,`股票简称`,`首次涨停时间`,result,result2
        | from detail_assemble_df order by buy_date""".stripMargin).show(1000)

    spark.sql(s"""select count(1),
                 |  sum(if($column>1,1,0)) as win_count,
                 |  round(sum(if($column>1,1,0))/count(1),2) as win_rate,
                 |  sum(if($column<=1,1,0)) as false_count,
                 |  round(sum(if($column<=1,1,0))/count(1),2) as false_rate
                 |  from detail_assemble_df where $column is not null""".stripMargin).show(1000)

    assemble_df.createOrReplaceTempView("assemble_df")
    println("================================================================assemble_df")
    spark.sql("select * from assemble_df order by m").show(1000)

    spark.sql(s"""select count(1),
                |  sum(if($column>1,1,0)) as win_count,
                |  round(sum(if($column>1,1,0))/count(1),2) as win_rate,
                |  sum(if($column<=1,1,0)) as false_count,
                |  round(sum(if($column<=1,1,0))/count(1),2) as false_rate
                |  from assemble_df where $column is not null""".stripMargin).show(1000)

    spark.sql(s"select round(EXP(SUM(LOG($column))),2) as result from assemble_df").show()





    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()

  }

  def create_table(spark:SparkSession,url:String,properties: Properties): Unit ={
    val jyrldf: DataFrame = spark.read.jdbc(url, "data_jyrl", properties)
    jyrldf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    jyrldf.createOrReplaceTempView("data_jyrl")

    val ztbdf: DataFrame = spark.read.jdbc(url, s"ztb_day", properties)
      .select("trade_date","`股票代码`","analysis").distinct()
    ztbdf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    ztbdf.createOrReplaceTempView("ztb_day")

    val g8_data_df: DataFrame = spark.read.jdbc(url, s"${table_name}", properties)
    g8_data_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    g8_data_df.createOrReplaceTempView(s"${table_name}")

    val wencaiquery_zt_zb_df: DataFrame = spark.read.jdbc(url, s"wencaiquery_zt_zb", properties)
    wencaiquery_zt_zb_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    wencaiquery_zt_zb_df.createOrReplaceTempView("wencaiquery_zt_zb")
  }

  /**
   * 明细表
   */


  /**
   * 胜率表
   */


  /**
   * 结果表
   */

}
