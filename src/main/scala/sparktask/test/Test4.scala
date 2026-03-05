package sparktask.test

import java.time.LocalTime
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel


import scala.collection.mutable.ArrayBuffer

/**
 *
 */
object Test4 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "400")
      .set("spark.sql.broadcastTimeout","60000")
      .set("spark.driver.memory", "12g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "10")
      .set("spark.local.dir", "D:\\SparkTemp")

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val url = "jdbc:mysql://192.168.0.100:3306/gs"
    val driver = "com.mysql.cj.jdbc.Driver"
    val user = "root"
    val pwd = "123456"

    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", pwd)
    properties.setProperty("url", url)
    properties.setProperty("driver", driver)
    val startm = System.currentTimeMillis()

    create_table(spark,url,properties)

//    val columnsList = ArrayBuffer[String]("stock_code","t0_trade_date","t1_trade_date","t2_trade_date","t3_trade_date",
//      "t4_trade_date","t5_trade_date","t6_trade_date","t7_trade_date","t8_trade_date","t9_trade_date",
//      "t0_close","t0_sfzt","t0_cjzt","t0_kxzt","t0_ln","t0_zrlnb","t0_qjzf","t0_stzf","t0_kpzf","t0_zgzf","t0_zdzf","t0_spzf",
//      "t1_close","t1_sfzt","t1_cjzt","t1_kxzt","t1_ln","t1_zrlnb","t1_qjzf","t1_stzf","t1_kpzf","t1_zgzf","t1_zdzf","t1_spzf",
//      "t2_close","t2_sfzt","t2_cjzt","t2_kxzt","t2_ln","t2_zrlnb","t2_qjzf","t2_stzf","t2_kpzf","t2_zgzf","t2_zdzf","t2_spzf"
//    )
    //select `股票代码`,`股票简称`,max_date,max(ztj) as ztj,max(xj) as xj,round((max(xj)-max(ztj))/max(ztj)*100,2) as zdf from
    //              (select ztb2.*,if(d2.t1_trade_date=ztb2.max_date,d2.t1_close,null) as ztj,if(d2.t1_trade_date='2026-02-02',d2.t1_close,null) as xj,t2_spzf from
    //                  (select `股票代码`,`股票简称`,max(trade_date) as max_date from ztb
    //                   where trade_date>='2025-12-31' and trade_date<='2026-02-02'
    //                   group by `股票代码`,`股票简称`) as ztb2
    //              left join (select * from data20_df where t1_trade_date>='2025-12-31' and t1_trade_date<='2026-02-02') as d2
    //              on ztb2.`股票代码` = d2.`stock_code`)
    //          where ztj is not null or xj is not null
    //          group by `股票代码`,`股票简称`,max_date

//    val df = spark.read.parquet(s"file:///D:\\gsdata2\\gpsj_hs_10days\\trade_date_month=202*").select(columnsList.map(col): _*)
//    df.createOrReplaceTempView("data20_df")
//    spark.sql(
//      """
//        |select *
//        | from data20_df where t1_trade_date>='2020-01-01' and t1_trade_date<='2025-12-31'
//        | and t9_trade_date is null
//        |""".stripMargin).orderBy("t1_trade_date").show(3000)

//    val df2 = spark.read.parquet(s"file:///D:\\gsdata2\\gpsj_day_all_hs")
//    df2.createOrReplaceTempView("ta2")
//    spark.sql(
//      """
//        |select * from ta2 where stock_code='601225' and trade_date in('2019-07-19','2019-07-22')
//        |""".stripMargin).show()



    // 设置交易时间范围（假设 09:30:00 到 15:00:00）
//    val startTime = LocalTime.parse("09:30:00")
//    val endTime = LocalTime.parse("15:00:00")
//    val numPartitions = 10 // 目标分区数，根据 MySQL 连接数调整
//
//    // 计算总秒数并生成分区谓词
//    val totalSeconds = java.time.Duration.between(startTime, endTime).getSeconds
//    val intervalSeconds = totalSeconds / numPartitions
//
//    val predicates = ArrayBuffer[String]()
//    for (i <- 0 until numPartitions) {
//      val lower = startTime.plusSeconds(i * intervalSeconds)
//      // 最后一个分区包含结束时间，其他分区使用半开区间 [lower, upper)
//      val upper = if (i == numPartitions - 1) endTime
//      else startTime.plusSeconds((i + 1) * intervalSeconds)
//
//      // time 字段为字符串类型 'HH:mm:ss'，构造 SQL 条件
//      val predicate = if (i == numPartitions - 1) {
//        s"time >= '${lower}' AND time <= '${upper}'"
//      } else {
//        s"time >= '${lower}' AND time < '${upper}'"
//      }
//      predicates += predicate
//    }
//
//    // 使用 predicates 并行读取数据
//    val df: DataFrame = spark.read.jdbc(url, "monitor_stock_adata_current_20260302", predicates.toArray, properties)
//
//    // 创建临时视图
//    df.createOrReplaceTempView("monitor_stock_adata_current")
//
//    // 验证分区数
//    println(s"实际分区数: ${df.rdd.getNumPartitions}")
    // 尝试解决字典编码兼容性问题
//    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
//    spark.conf.set("spark.sql.parquet.filterPushdown", "false")
//    // 设置输出压缩为 snappy
//    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
//    val data_df = spark.read.parquet(s"file:///D:\\gsdata2\\gpsj_hs_10days_before")
//    data_df.printSchema()
//    data_df.show()
//    data_df.write
//      .mode("overwrite").partitionBy("trade_date_month")    // 按需选择模式
//      .parquet(s"file:///D:\\gsdata2\\gpsj_hs_10days")


    xq1(spark,url,properties)
    //2、
//    xq2(spark:SparkSession,url,properties)
    


    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
  }
  def create_table(spark:SparkSession,url:String,properties: Properties): Unit ={
    // 假设读取后得到 DataFrame df
    val monitor_df = spark.read.jdbc(url, "monitor_stock_adata_current_20260303", properties)
    monitor_df.createOrReplaceTempView("monitor_stock_adata_current")
  }
  
  def xq1(spark:SparkSession,url:String,properties: Properties): Unit ={
    val df = spark.sql(
      """
        |WITH prev_change AS (
        |    -- 获取每个股票上一时间点的 change_pct
        |    SELECT
        |        time,
        |        stock_code,
        |        change_pct,
        |        LAG(change_pct) OVER (PARTITION BY stock_code ORDER BY time) AS prev_change_pct,
        |        LAG(volume) OVER (PARTITION BY stock_code ORDER BY time) AS prev_volume
        |    FROM monitor_stock_adata_current
        |),
        |change_direction AS (
        |    -- 计算方向（排除第一条无对比数据的记录）
        |    SELECT
        |        time,
        |        CASE
        |            WHEN change_pct > prev_change_pct THEN 'up'
        |            WHEN change_pct < prev_change_pct THEN 'down'
        |            ELSE 'flat'
        |        END AS direction,
        |        CASE
        |            WHEN volume > prev_volume THEN 'up'
        |            WHEN volume < prev_volume THEN 'down'
        |            ELSE 'flat'
        |        END AS volume_direction
        |    FROM prev_change
        |    WHERE prev_change_pct IS NOT NULL   -- 忽略每个股票的第一条记录
        |)
        |SELECT
        |    time,
        |    COUNT(*) AS total_stocks,
        |    SUM(CASE WHEN direction = 'up' THEN 1 ELSE 0 END) AS up_count,
        |    SUM(CASE WHEN direction = 'down' THEN 1 ELSE 0 END) AS down_count,
        |    SUM(CASE WHEN direction = 'flat' THEN 1 ELSE 0 END) AS flat_count,
        |    ROUND(SUM(CASE WHEN direction = 'up' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS up_pct,
        |    ROUND(SUM(CASE WHEN direction = 'down' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS down_pct,
        |    ROUND(SUM(CASE WHEN direction = 'flat' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS flat_pct,
        |    ROUND((SUM(CASE WHEN direction = 'up' THEN 1 ELSE 0 END) / COUNT(*))/(SUM(CASE WHEN direction = 'down' THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) as up_down_pct
        |FROM change_direction
        |GROUP BY time
        |ORDER BY time
        |""".stripMargin)

    df.createOrReplaceTempView("ta1")

    val df2 =spark.sql("""
                         |select time,count(1) as zs,sum(if(change_pct>0,1,0)) as sz,sum(if(change_pct<0,1,0)) as xd,sum(if(change_pct=0,1,0)) as p,
                         |       ROUND(sum(if(change_pct>0,1,0))/count(1) * 100, 2) as sz_pct,
                         |       ROUND(sum(if(change_pct<0,1,0))/count(1) * 100, 2) as xd_pct,
                         |       ROUND(sum(if(change_pct=0,1,0))/count(1) * 100, 2) as p_pct
                         | from monitor_stock_adata_current group by time order by time
                         |""".stripMargin)

    df2.createOrReplaceTempView("ta2")
    val df3 = spark.sql(
      """
        |select * from ta1  join ta2 using(time) order by ta1.time
        |""".stripMargin)
    df3.write.mode("overwrite").jdbc(url,"aaa_test1",properties)
  }
  
  def xq2(spark:SparkSession,url:String,properties: Properties): Unit ={
    val df4 = spark.sql(
      """
        |-- 计算30秒涨速
        |WITH zs AS (
        |    SELECT 
        |        a.time,
        |        a.stock_code,
        |        (a.change_pct - b.change_pct) AS `zs_30s`   -- 30秒涨速
        |    FROM monitor_stock_adata_current as a
        |    INNER JOIN monitor_stock_adata_current as b 
        |        ON a.stock_code = b.stock_code 
        |        AND b.time = a.time - INTERVAL 30 SECOND   -- 精确匹配30秒前的记录
        |),
        |-- 涨幅前10
        |gain_top10 AS (
        |    SELECT time, stock_code, `zs_30s`,
        |           ROW_NUMBER() OVER (PARTITION BY time ORDER BY `zs_30s` DESC) AS rn
        |    FROM zs
        |),
        |-- 跌幅前10
        |loss_top10 AS (
        |    SELECT time, stock_code, `zs_30s`,
        |           ROW_NUMBER() OVER (PARTITION BY time ORDER BY `zs_30s` ASC) AS rn
        |    FROM zs
        |)
        |-- 合并结果，允许同一时间同一股票同时出现在涨幅和跌幅（如涨速为0且两侧数量不足时）
        |SELECT time, stock_code, `zs_30s`
        |FROM gain_top10 WHERE rn <= 10
        |UNION ALL
        |SELECT time, stock_code, `zs_30s`
        |FROM loss_top10 WHERE rn <= 10
        |ORDER BY time, `zs_30s` DESC   -- 按时间升序，涨速从高到低展示
        |""".stripMargin)
    
    df4.show()
  }
}
