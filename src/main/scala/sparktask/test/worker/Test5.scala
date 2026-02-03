package sparktask.test.worker

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object Test5 {
  def main(args: Array[String]): Unit = {
    val startm = System.currentTimeMillis()
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "20")
      .set("spark.sql.broadcastTimeout","60000")
      .set("spark.driver.memory", "4g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "20")
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

//    val df = spark.read.json("file:///C:\\Users\\Administrator\\Desktop\\area.txt")
//
//     df.createOrReplaceTempView("ta1")
    import spark.implicits._
//
//    val tablename = "news_area"
//
//    val result = df
//      .select(explode($"全球领域集合").as("domain"))
//      .select(
//        $"domain.主领域".as("main_area"),
//        explode($"domain.子领域").as("child_area")
//      )
//
//    result.write.jdbc(url, tablename, properties)


    val df = spark.read.jdbc(url,"analysis_area",properties)
//    df.createOrReplaceTempView("ta1")

    // 方法1：使用from_json和explode
    val schema = ArrayType(StructType(Seq(
      StructField("时间", StringType, true),
      StructField("主领域", StringType, true),
      StructField("子领域", StringType, true),
      StructField("关键事件", StringType, true),
      StructField("简要描述", StringType, true),
      StructField("利空利好", StringType, true),
      StructField("涉及板块", StringType, true),
      StructField("股票代码", StringType, true)
    )))

    val resultDF = df
      .select(from_json($"json_data", StructType(Seq(
        StructField("消息集合", schema, true)
      ))).as("parsed_json"))
      .select(explode($"parsed_json.消息集合").as("message"))
      .select(
        $"message.时间".as("时间"),
        $"message.主领域".as("主领域"),
        $"message.子领域".as("子领域"),
        $"message.关键事件".as("关键事件"),
        $"message.简要描述".as("简要描述"),
        $"message.利空利好".as("利空利好"),
        $"message.涉及板块".as("涉及板块"),
        $"message.股票代码".as("股票代码")
      )

    // 显示结果
    resultDF
//      .where("`利空利好`='利好'")
      .createOrReplaceTempView("ta1")
//    resultDF.printSchema()

    spark.sql(
      """
        |select bk,sum(if(`利空利好`='利好',1,0)) as c_lh,sum(if(`利空利好`='利空',1,0)) as c_lk,
        | sum(if(`利空利好`='利好',1,0))-sum(if(`利空利好`='利空',1,0)) as c,
        | sum(if(`利空利好`='利好',1,0))/sum(if(`利空利好`='利空',1,0)) as b
        | from
        |     (select `利空利好`,explode(split(`涉及板块`,',')) as bk from ta1 where `时间`='2025-11-19')
        |group by bk
        |
        |
        |""".stripMargin).orderBy(col("b").desc).show()

    spark.sql(
      """
        |
        |""".stripMargin)
  }
}
