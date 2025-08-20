package sparktask.test

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
 * ai analysis to wide_table
 */
object ReadTest4 {
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

    import spark.implicits._

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

    // ===================================================================================
    val df: DataFrame = spark.read.jdbc(url, "analysis_news", properties).limit(20)

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

    // 5. 显示结果
//    println("转换后的数据结构:")
//    resultDf.printSchema()

    println("\n结果数据:")
    resultDf.createOrReplaceTempView("dwd_news")
    resultDf.show(20, truncate = false)

    val news_cls_df: DataFrame = spark.read.jdbc(url, "news_cls", properties).where("analysis='1'")
      .select("内容hash","发布时间","标题","内容").toDF("key","time","title","content")
    val news_combine_df: DataFrame = spark.read.jdbc(url, "news_combine", properties).where("analysis='1'")
      .select("内容hash","发布时间","标题","内容").toDF("key","time","title","content")
    val news_financial_df: DataFrame = spark.read.jdbc(url, "news_financial", properties).where("analysis='1'")
      .select("内容hash","时间","标题","内容").toDF("key","time","title","content")

    val news_df = news_cls_df.union(news_combine_df).union(news_financial_df)
    news_df.createOrReplaceTempView("news_df")

    spark.sql("""select * from news_df left join dwd_news on news_df.key=dwd_news.`消息id` where `消息id` is not null""")
      .drop("消息id")
      .write.mode("overwrite").parquet("file:///D:\\gsdata\\analysis_news")


    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }
}
