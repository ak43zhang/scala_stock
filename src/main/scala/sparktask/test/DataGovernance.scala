package sparktask.test

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, explode, from_json}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.functions.lit


/**
 * 数据治理
 */
object DataGovernance {

  val data_content = "gsdata2"
  val start_time = "2025-01-01"
  val end_time = "2025-01-01"

  def main(args: Array[String]): Unit = {
    val startm = System.currentTimeMillis()

    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "60")
      .set("spark.sql.broadcastTimeout","60000")
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

    val properties = getMysqlProperties()

    val year = end_time.substring(0,4)

    analysis_news2Parquet(spark,properties,year)



    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }

  def getMysqlProperties(): Properties ={
    val url = "jdbc:mysql://localhost:3306/gs"
    val driver = "com.mysql.cj.jdbc.Driver"
    val user = "root"
    val pwd = "123456"

    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", pwd)
    properties.setProperty("url", url)
    properties.setProperty("driver", driver)
    properties
  }

  def analysis_news2Parquet(spark:SparkSession,properties: Properties,year:String): Unit ={
    val url = properties.getProperty("url")
    import spark.implicits._
    val df: DataFrame = spark.read.jdbc(url, "analysis_news"+year, properties)

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
      .select(explode($"parsed_json.消息集合").as("message"),col("table_name"),col("json_value"))

    // 4. 展开嵌套结构并选择所需字段
    val resultDf = parsedDf
      .select(
        col("table_name"),
        col("json_value"),
        $"message.消息id".as("消息id"),
        $"message.消息大小".as("消息大小"),
        $"message.消息类型".as("消息类型"),
        concat_ws(",", $"message.涉及板块").as("涉及板块"),
        concat_ws(",", $"message.龙头个股").as("龙头个股"),
        explode($"message.板块详情").as("板块详情")
      )

    resultDf.createOrReplaceTempView("dwd_news")

    spark.sql(
      """
        |select json_value,first(`消息id`) as ids from dwd_news group by json_value
        |""".stripMargin).createOrReplaceTempView("dwd_news2")

//    val news_cls_df: DataFrame = spark.read.jdbc(url, "news_cls"+year, properties).where("analysis='1'")
//      .select("内容hash","发布时间","标题","内容").toDF("key","time","title","content")
//      .withColumn("table_name",lit("news_cls"+year))
//
//    val news_df = news_cls_df
//    news_df.createOrReplaceTempView("news_df")
//
//    val cls_result_df = spark.sql(
//      """select news_df.table_name,dwd_news2.json_value
//        |from dwd_news2 left join news_df on news_df.key=dwd_news2.ids""".stripMargin)
//
////    cls_result_df.show()
//    cls_result_df.write.mode("append").jdbc(url,"analysis_news2027",properties)

    val news_combine_df: DataFrame = spark.read.jdbc(url, "news_combine"+year, properties).where("analysis='1'")
      .select("内容hash","发布时间","标题","内容").toDF("key","time","title","content")
      .withColumn("table_name",lit("news_combine"+year))

    val news_df2 = news_combine_df
    news_combine_df.createOrReplaceTempView("news_df2")

    val combine_result_df = spark.sql(
      """select news_df2.table_name,dwd_news2.json_value
        |from dwd_news2 left join news_df2 on news_df2.key=dwd_news2.ids""".stripMargin)
        .where("table_name='news_combine2025'")

    combine_result_df.show()
//    combine_result_df.write.mode("append").jdbc(url,"analysis_news2027",properties)

//    println("news AI分析当前数据量"+resultdf.count())
//    resultdf.write.mode("overwrite").parquet(s"file:///D:\\${data_content}\\analysis_news")
  }
}
