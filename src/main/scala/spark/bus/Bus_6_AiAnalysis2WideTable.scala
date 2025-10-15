package spark.bus

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 总线6
 * 将ai分析出来的数据制作成宽表parquet
 * 后期补录数据
 */
object Bus_6_AiAnalysis2WideTable {

  val start_time = "2025-10-10"
  val end_time = "2025-10-15"

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

    analysis_news2Parquet(spark,url,properties)
    analysis_notices2Parquet(spark,url,properties)
    analysis_ztb2Parquet(spark,url,properties)
    analysis2news(spark)
    analysis2notices(spark)
    analysis2ztb(spark)

    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }

  def analysis_news2Parquet(spark:SparkSession,url:String,properties: Properties): Unit ={
    import spark.implicits._
    val df: DataFrame = spark.read.jdbc(url, "analysis_news", properties)

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

    val news_cls_df: DataFrame = spark.read.jdbc(url, "news_cls", properties).where("analysis='1'")
      .select("内容hash","发布时间","标题","内容").toDF("key","time","title","content")
    val news_combine_df: DataFrame = spark.read.jdbc(url, "news_combine", properties).where("analysis='1'")
      .select("内容hash","发布时间","标题","内容").toDF("key","time","title","content")
    val news_financial_df: DataFrame = spark.read.jdbc(url, "news_financial", properties).where("analysis='1'")
      .select("内容hash","时间","标题","内容").toDF("key","time","title","content")

    val news_df = news_cls_df.union(news_combine_df).union(news_financial_df)
    news_df.createOrReplaceTempView("news_df")

    val resultdf = spark.sql("""select * from news_df left join dwd_news on news_df.key=dwd_news.`消息id` where `消息id` is not null""")
      .drop("消息id")
    println("news AI分析当前数据量"+resultdf.count())
    resultdf.write.mode("overwrite").parquet("file:///D:\\gsdata\\analysis_news")
  }

  def analysis_notices2Parquet(spark:SparkSession,url:String,properties: Properties): Unit ={
    import spark.implicits._
    val df: DataFrame = spark.read.jdbc(url, "analysis_notices", properties)

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
    resultdf.write.mode("overwrite").parquet("file:///D:\\gsdata\\analysis_notices")
  }

  def analysis_ztb2Parquet(spark:SparkSession,url:String,properties: Properties): Unit ={
    val df: DataFrame = spark.read.jdbc(url, "analysis_ztb", properties)
    df.createOrReplaceTempView("ztb")
    val resultdf = spark.sql(
      """
        |select *,get_json_object(json_data, '$.股票名称') AS stock_name,
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
//    resultdf.show(false)
    resultdf.write.mode("overwrite").parquet("file:///D:\\gsdata\\analysis_ztb")


    println("ztb AI分析当前数据量"+resultdf.count())
  }

  /**
   * 分析news
   */
  def analysis2news(spark:SparkSession): Unit ={
    val news_df = spark.read.parquet("file:///D:\\gsdata\\analysis_news")
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
    val notice_df = spark.read.parquet("file:///D:\\gsdata\\analysis_notices")
          .where(s"time between '${start_time}' and '${end_time}'")
    notice_df.createOrReplaceTempView("notices")

    println("======================notices===========================")
    val notice_risk_df = spark.sql(
      """
        |select * from notices
        |where `消息类型` = '利好'
        |
        |""".stripMargin)
    notice_risk_df.show()

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
    val ztb_df = spark.read.parquet("file:///D:\\gsdata\\analysis_ztb").drop("json_data")
    //      .where("time between '2025-06-03' and '2025-06-04'")
    ztb_df.createOrReplaceTempView("ztb")

//    ztb_df.printSchema()

    println("======================ztb===========================")
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
