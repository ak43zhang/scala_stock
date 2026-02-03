package spark.bus

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.ParameterSet
import spark.tools.MysqlProperties
import sparktask.tools.MysqlTools

/**
 * 总线6
 * 将ai分析出来的数据制作成宽表parquet
 * 后期补录数据
 */
object Bus_6_AiAnalysis2WideTable {

  val start_time = "2025-01-01"
  val end_time = "2026-01-30"

  def main(args: Array[String]): Unit = {
    val startm = System.currentTimeMillis()

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

    val properties = MysqlProperties.getMysqlProperties()

    val year = end_time.substring(0,4)

//    analysis_news2Parquet(spark,properties,year)
    analysis_notices2Parquet(spark,properties,year)
//    analysis_ztb2Parquet(spark,properties,year)
    analysis_area2Parquet(spark,properties,year)

//    analysis2news(spark)
//    analysis2notices(spark)
//    analysis2ztb(spark)

    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }

  def analysis_news2Parquet(spark:SparkSession,properties: Properties,year:String): Unit ={
    val url = properties.getProperty("url")
    import spark.implicits._
    val df: DataFrame = spark.read.jdbc(url, "analysis_news"+year, properties)
      .union(spark.read.jdbc(url, "analysis_news2025", properties))

    val stockDetailSchema = new StructType()
      .add("a股代码", StringType)
      .add("a股名称", StringType)
      .add("关联原因", StringType)
      .add("利好利空", StringType)

    val sectorDetailSchema = new StructType()
      .add("板块名称", StringType)
      .add("板块明细", ArrayType(stockDetailSchema))

    val messageSchema = new StructType()
      .add("消息id", StringType)
      .add("板块详情", ArrayType(sectorDetailSchema))
      .add("消息大小", StringType)
      .add("消息类型", StringType)
      .add("涉及板块", ArrayType(StringType))
      .add("龙头个股", ArrayType(StringType))

    val rootSchema = new StructType()
      .add("消息集合", ArrayType(messageSchema))

    // 3. 解析JSON字符串并展开嵌套结构
    val parsedDf = df
      .withColumn("parsed_json", from_json($"json_value", rootSchema))
      .select(explode($"parsed_json.消息集合").as("message"),col("table_name"))

    // 4. 展开嵌套结构并选择所需字段
    val resultDf = parsedDf
      .select(
        col("table_name"),
        $"message.消息id".as("消息id"),
        $"message.消息大小".as("消息大小"),
        $"message.消息类型".as("消息类型"),
        concat_ws(",", $"message.涉及板块").as("涉及板块"),
        concat_ws(",", $"message.龙头个股").as("龙头个股"),
        explode($"message.板块详情").as("板块详情")
      )
      .select(
        $"table_name",
        $"消息id",
        $"消息大小",
        $"消息类型",
        $"涉及板块",
        $"龙头个股",
        $"板块详情.板块名称".as("板块名称"),
        explode($"板块详情.板块明细").as("板块明细")
      )
      .select(
        $"table_name",
        $"消息id",
        $"消息大小",
        $"消息类型",
        $"涉及板块",
        $"龙头个股",
        $"板块名称",
        $"板块明细.a股代码".as("a股代码"),
        $"板块明细.a股名称".as("a股名称"),
        $"板块明细.关联原因".as("关联原因"),
        $"板块明细.利好利空".as("利好利空")
      )


    resultDf.createOrReplaceTempView("dwd_news")

    val news_cls_df: DataFrame = spark.read.jdbc(url, "news_cls"+year, properties).where("analysis='1'")
      .select("内容hash","发布时间","标题","内容").toDF("key","time","title","content")
    val news_combine_df: DataFrame = spark.read.jdbc(url, "news_combine"+year, properties).where("analysis='1'")
      .select("内容hash","发布时间","标题","内容").toDF("key","time","title","content")
    //    val news_financial_df: DataFrame = spark.read.jdbc(url, "news_financial", properties).where("analysis='1'")
    //      .select("内容hash","时间","标题","内容").toDF("key","time","title","content")

    val news_df = news_cls_df.union(news_combine_df)
    //      .union(news_financial_df)
    news_df.createOrReplaceTempView("news_df")

    val resultdf = spark.sql("""select * from news_df left join dwd_news on news_df.key=dwd_news.`消息id` where `消息id` is not null""")
      .drop("消息id")
    println("news AI分析当前数据量"+resultdf.count())
    resultdf.write.mode("overwrite").parquet(s"file:///D:\\${ParameterSet.data_content}\\analysis_news")
  }

  def analysis_notices2Parquet(spark:SparkSession,properties: Properties,year:String): Unit ={
    val url = properties.getProperty("url")
    import spark.implicits._
    val df: DataFrame = spark.read.jdbc(url, "analysis_notices"+year, properties)
      .union(spark.read.jdbc(url, "analysis_notices2025", properties))

    val messageSchema = new StructType()
      .add("公告id", StringType)
      .add("公告日期", StringType)
      .add("股票代码", StringType)
      .add("风险大小", StringType)
      .add("消息类型", StringType)
      .add("判定依据", ArrayType(StringType))

    val rootSchema = new StructType()
      .add("公告集合", ArrayType(messageSchema))

    // 3. 解析JSON字符串并展开嵌套结构
    val parsedDf = df
      .withColumn("parsed_json", from_json($"json_value", rootSchema))
      .select(explode($"parsed_json.公告集合").as("message"),col("table_name"))

    // 4. 展开嵌套结构并选择所需字段
    val resultDf = parsedDf
      .select(
        col("table_name"),
        $"message.公告id".as("公告id"),
        $"message.公告日期".as("公告日期"),
        $"message.股票代码".as("股票代码"),
        $"message.风险大小".as("风险大小"),
        $"message.消息类型".as("消息类型"),
        concat_ws(",", $"message.判定依据").as("判定依据")
      )
      .select(
        $"table_name",
        $"公告id",
        $"公告日期",
        $"股票代码",
        $"风险大小",
        $"消息类型",
        $"判定依据"
      )

    resultDf.createOrReplaceTempView("dwd_notices")

    val notices_df: DataFrame = spark.read.jdbc(url, "jhsaggg2025", properties).where("analysis='1'")
      .select("内容hash","公告日期","公告标题").toDF("key","time","title")

    notices_df.createOrReplaceTempView("notices_df")

    val resultdf = spark.sql("""select * from notices_df left join dwd_notices on notices_df.key=dwd_notices.`公告id` where `公告id` is not null""")
      .drop("公告id")
//    resultdf.show(false)
    println("notices AI分析当前数据量"+resultdf.count())
    resultdf.write.mode("overwrite").parquet(s"file:///D:\\${ParameterSet.data_content}\\analysis_notices")
  }

  def analysis_ztb2Parquet(spark:SparkSession,properties: Properties,year:String): Unit ={
    val url = properties.getProperty("url")
    val df: DataFrame = spark.read.jdbc(url, "analysis_ztb"+year, properties)
      .union(spark.read.jdbc(url, "analysis_ztb2025", properties))
    df.createOrReplaceTempView("ztb")
    val resultdf = spark.sql(
      """
        |select gpjc_sj_id,gpjc,sj,get_json_object(json_data, '$.股票名称') AS stock_name,
        |get_json_object(json_data, '$.涨停时间') AS zt_time,
        |get_json_object(json_data, '$.股性分析') AS gxfx,
        |from_json(get_json_object(json_data, '$.消息'),'ARRAY<STRUCT<`影响消息`: STRING, `最早出现时间`: STRING>>') AS xx,
        |from_json(get_json_object(json_data, '$.板块消息'),'ARRAY<STRUCT<`板块`: STRING, `板块刺激消息`: ARRAY<STRING>>>') AS bkxx,
        |from_json(get_json_object(json_data, '$.概念消息'),'ARRAY<STRUCT<`概念`: STRING, `概念刺激消息`: ARRAY<STRING>>>') AS gnxx,
        |from_json(get_json_object(json_data, '$.龙头股消息'),'ARRAY<STRUCT<`龙头股`: STRING, `龙头股刺激消息`: ARRAY<STRING>>>') AS ltgxx,
        |from_json(get_json_object(json_data, '$.预期涨停消息'),'ARRAY<STRUCT<`延续性`: STRING,`最早出现时间`: STRING, `预期消息`:STRING>>') AS yqztxx
        |
        | from ztb
        |""".stripMargin)
    resultdf.show(1,false)

//    resultdf.write.mode("overwrite").parquet(s"file:///D:\\${ParameterSet.data_content}\\analysis_ztb")


    println("ztb AI分析当前数据量"+resultdf.count())
  }

//  TODO analysis_area
  def analysis_area2Parquet(spark:SparkSession,properties: Properties,year:String): Unit ={
    val url = properties.getProperty("url")
    val df: DataFrame = spark.read.jdbc(url, "analysis_area"+year, properties)
      .union(spark.read.jdbc(url, "analysis_area2025", properties))
    df.createOrReplaceTempView("area")
    val resultdf = spark.sql(
      """
        |SELECT
        |    news_date,
        |    main_area,
        |    child_area,
        |    xxjh_exploded.`主领域` AS `主领域`,
        |    xxjh_exploded.`关键事件` AS `关键事件`,
        |    xxjh_exploded.`利空利好` AS `利空利好`,
        |    xxjh_exploded.`消息大小` AS `消息大小`,
        |    xxjh_exploded.`子领域` AS `子领域`,
        |    xxjh_exploded.`时间` AS `时间`,
        |    xxjh_exploded.`涉及板块` AS `涉及板块`,
        |    xxjh_exploded.`简要描述` AS `简要描述`,
        |    xxjh_exploded.`股票代码` AS `股票代码`,
        |    xxjh_exploded.`原因分析` AS `原因分析`
        |FROM (select news_date,main_area,child_area,
        |from_json(get_json_object(json_data, '$.消息集合'),'ARRAY<STRUCT<`主领域`: STRING,`关键事件`: STRING,`利空利好`: STRING,`子领域`: STRING,`时间`: STRING,`涉及板块`: STRING,`简要描述`: STRING, `股票代码`:STRING, `原因分析`:STRING, `消息大小`:STRING>>') AS xxjh
        | from area)
        |LATERAL VIEW EXPLODE(xxjh) exploded_table AS xxjh_exploded
        |
        |""".stripMargin)

    resultdf.createOrReplaceTempView("ta1")


    //领域sql
    val withsql =
      s"""
         |WITH processed_data AS (
         |  SELECT
         |    `股票代码`,
         |    regexp_replace(regexp_replace(`股票代码`,'[^0-9,，]',''),'，',',') AS cleaned_codes
         |  FROM ta1
         |  WHERE news_date between '${start_time}' and '${end_time}' and `利空利好`='利好' and `消息大小` in ('重大','大') and
         |  `股票代码` IS NOT NULL AND `股票代码` != ''
         |),
         |exploded_codes AS (
         |  SELECT
         |    trim(exploded_code) AS stock_code,
         |    length(trim(exploded_code)) as code_len
         |  FROM processed_data
         |  LATERAL VIEW explode(
         |    filter(split(cleaned_codes, ','),x -> trim(x) != '')
         |  ) t AS exploded_code
         |)
         |""".stripMargin
    val selectsql =
      """
        |
        |SELECT
        |  stock_code,
        |  COUNT(*) AS frequency,
        |  -- 如果需要，可以计算占比
        |  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
        |FROM exploded_codes
        |WHERE code_len = 6  -- 只统计6位数
        |  AND stock_code REGEXP '^[0-9]{6}$'  -- 确保是6位纯数字
        |  and (stock_code like '00%' or stock_code like '60%' or stock_code like '30%')
        |GROUP BY stock_code
        |ORDER BY frequency DESC, stock_code
        |""".stripMargin
    val area_sql = withsql+selectsql
    //      println(ressql)

//    spark.sql(area_sql).show(2000)



//    resultdf.show(false)
//    resultdf.printSchema()

    val table = "analysis_area_explode"

    resultdf.distinct().write.mode("overwrite").jdbc(url, table, properties)

    println("analysis_area_explode AI分析当前数据量"+resultdf.count())
  }

  /**
   * 分析news
   */
  def analysis2news(spark:SparkSession): Unit ={
    val news_df = spark.read.parquet(s"file:///D:\\${ParameterSet.data_content}\\analysis_news")
    news_df.where(s"time between '${start_time} 15:00:00' and '${end_time} 09:15:00' ")
      .createOrReplaceTempView("news")

    println("======================news===========================")

    //板块分析
    spark.sql(
      """
        |select bk,count(1) from
        |   (select bk from
        |       (select distinct key,split(`涉及板块`,',') as bks from news where `消息大小` in ('重大','大') and `消息类型` = '利好')
        |   lateral view explode(bks) as bk)
        |group by bk order by count(1) desc
        |""".stripMargin).show()

    spark.sql(
      """
        |select * from news where `消息大小` in ('重大','大') and `消息类型` = '利好'
        |""".stripMargin).show()


//    news_df.show(false)
  }

  /**
   * 分析notices
   */
  def analysis2notices(spark:SparkSession): Unit ={
    val notice_df = spark.read.parquet(s"file:///D:\\${ParameterSet.data_content}\\analysis_notices")
          .where(s"time between '${start_time}' and '${end_time}'")
    notice_df.createOrReplaceTempView("notices")

    println("======================notices===========================")
    val notice_risk_df = spark.sql(
      """
        |select * from notices
        |where `消息类型` = '利好'
        |
        |""".stripMargin)
//    notice_risk_df.show()

//    notice_df.groupBy("风险大小","消息类型").agg(count("*").alias("count"))
//      .orderBy("风险大小","消息类型").show(100,false)
//
//    spark.sql(
//      """
//        |select * from notices where `风险大小`='重大' and `消息类型`='利好'  order by time desc
//        |""".stripMargin).show(100,false)
//
//    spark.sql(
//      """
//        |select time,`风险大小`,`消息类型`,count(1) from notices group by time,`风险大小`,`消息类型` order by time desc,`风险大小`,`消息类型`
//        |""".stripMargin).show(200,false)

//    notice_df.show()
  }


  /**
   * 分析涨停板
   */
  def analysis2ztb(spark:SparkSession): Unit ={
    val ztb_df = spark.read.parquet(s"file:///D:\\${ParameterSet.data_content}\\analysis_ztb").drop("json_data")
    //      .where("time between '2025-06-03' and '2025-06-04'")
    ztb_df.createOrReplaceTempView("ztb")

//    ztb_df.printSchema()

    println("======================ztb===========================")
    spark.sql(
      s"""
        |select * from ztb where sj between '${start_time}' and '${end_time}'
        |""".stripMargin).show(10,false)
    spark.sql(
      s"""
        |select bk,count(1) from ztb lateral view explode(bkxx.`板块`) as bk where sj between '${start_time}' and '${end_time}' group by bk order by count(1) desc
        |""".stripMargin).show()

    spark.sql(
      s"""
        |select gn,count(1) from ztb lateral view explode(gnxx.`概念`) as gn where sj between '${start_time}' and '${end_time}' group by gn order by count(1) desc
        |""".stripMargin).show()

//    ztb_df.show(false)
  }
}
