package spark.email

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.tools.MysqlProperties
import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.sql.functions._
import sparktask.tools.Send163EmailTools


object SendEmail_ztb {
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

    val send_ztb_date = "2026-03-04"

    send_email(spark,send_ztb_date)

  }

  def send_email(spark:SparkSession,send_ztb_date:String): Unit ={
    val year = send_ztb_date.substring(0,4)
    val formatStockInfoUDF = udf(formatStockInfo _)

    val properties = MysqlProperties.getMysqlProperties()
    // 注册UDF格式化函数
    spark.udf.register("format_stock_info", formatStockInfoUDF)

    val url = properties.getProperty("url")
    val df: DataFrame = spark.read.jdbc(url, "analysis_ztb"+year, properties)
    //    df.printSchema()
    //    df.createOrReplaceTempView("ztb")
    //    df.show(1,false)
    // 应用UDF
    val result = df.selectExpr(
      "gpjc_sj_id","gpjc","sj","format_stock_info(json_data) as formatted_info"
    ).where(s"sj = '$send_ztb_date' and gpjc not like '%ST%'")
    println(result.count)

    val regex = """\[(citation:\d+)\]|\((citation:\d+)\)|\[reference:\d+\]""".r

    val concatenatedString = result
      .select("formatted_info")
      .collect()  // 收集到Driver端
      .map(row => row.getString(0))  // 提取字符串值
      .mkString("\n\n\n")  // 用三个换行符拼接

    val result_content = regex.replaceAllIn(concatenatedString, "")
    println(result_content)

    // 配置信息（需替换为实际值）
    val username = "m17600700886@163.com"  // 你的163邮箱
    val password = "CPPHA5yTAZQLDju5" // 邮箱SMTP授权码（非登录密码）
    val to = "m17600700886@163.com"      // 收件人邮箱

    val subject = s"${send_ztb_date}涨停板分析"

    // 发送文本邮件
//    Send163EmailTools.sendEmail(username, password, to, subject ,result_content)
  }

  // UDF函数定义（修复版本）
  def formatStockInfo(jsonStr: String): String = {
    if (jsonStr == null || jsonStr.trim.isEmpty) {
      return "输入为空"
    }

    implicit val formats: DefaultFormats = DefaultFormats

    // 解析JSON
    val json = parse(jsonStr)

    // 提取字段，添加默认值处理
    val stockName = (json \ "股票名称").extractOrElse[String]("")
    val limitUpTime = (json \ "涨停时间").extractOrElse[String]("")
    val gxfx = (json \ "股性分析").extractOrElse[String]("")
    val lhbfx = (json \ "龙虎榜分析").extractOrElse[String]("")

    // 处理板块消息
    val blockMsgs = (json \ "板块消息").extractOrElse[List[Map[String, Any]]](List.empty)
    val blockFormatted = formatArray(blockMsgs, "板块", "板块刺激消息", "板块")

    // 处理概念消息
    val conceptMsgs = (json \ "概念消息").extractOrElse[List[Map[String, Any]]](List.empty)
    val conceptFormatted = formatArray(conceptMsgs, "概念", "概念刺激消息", "概念")

    // 处理龙头股消息
    val leaderMsgs = (json \ "龙头股消息").extractOrElse[List[Map[String, Any]]](List.empty)
    val leaderFormatted = formatArray(leaderMsgs, "龙头股", "龙头股刺激消息", "龙头股")

    // 处理消息（按最早出现时间排序）
    val messages = (json \ "消息").extractOrElse[List[Map[String, String]]](List.empty)
    val sortedMessages = messages.sortBy(_.getOrElse("最早出现时间", ""))
    val messageFormatted = formatSimpleArray(sortedMessages, "影响消息", "消息")

    // 处理预期涨停消息（按最早出现时间排序）
    val expectedMsgs = (json \ "预期涨停消息").extractOrElse[List[Map[String, String]]](List.empty)
    val sortedExpected = expectedMsgs.sortBy(_.getOrElse("最早出现时间", ""))
    val expectedFormatted = formatExpectedArray(sortedExpected)

    // 处理深度分析
    val depthAnalysis = (json \ "深度分析").extractOrElse[List[String]](List.empty)
    val depthAnalysisFormatted = formatDepthAnalysis(depthAnalysis)

    // 拼接所有部分，只添加非空部分
    val resultBuilder = List.newBuilder[String]

    // 总是添加股票名称和涨停时间
    resultBuilder += s"股票名称: $stockName,"
    resultBuilder += s"涨停时间: $limitUpTime,"
    resultBuilder += s"股性分析: $gxfx,"
    resultBuilder += s"龙虎榜分析: $lhbfx,"

    // 只添加有内容的板块消息
    if (blockFormatted.nonEmpty) {
      resultBuilder ++= blockFormatted
    }

    // 只添加有内容的概念消息
    if (conceptFormatted.nonEmpty) {
      resultBuilder ++= conceptFormatted
    }

    // 只添加有内容的龙头股消息
    if (leaderFormatted.nonEmpty) {
      resultBuilder ++= leaderFormatted
    }

    // 只添加有内容的消息
    if (messageFormatted.nonEmpty) {
      resultBuilder += messageFormatted
    }

    // 只添加有内容的预期涨停消息
    if (expectedFormatted.nonEmpty) {
      resultBuilder += expectedFormatted
    }

    // 只添加有内容的深度分析
    if (depthAnalysisFormatted.nonEmpty) {
      resultBuilder ++= depthAnalysisFormatted
    }

    // 用换行连接所有部分
    resultBuilder.result().mkString("\n")
  }


  // 格式化数组方法（修复版本）
  private def formatArray(
                           items: List[Map[String, Any]],
                           keyField: String,
                           msgField: String,
                           prefix: String
                         ): List[String] = {
    items.zipWithIndex.map { case (item, index) =>
      val key = item.getOrElse(keyField, "").toString
      val messages = item.get(msgField) match {
        case Some(list: List[_]) => list.map(_.toString).mkString("、")
        case _ => ""
      }
      s"$prefix：${index + 1}、$key————${prefix}刺激消息：$messages"
    }
  }

  private def formatSimpleArray(
                                 items: List[Map[String, String]],
                                 msgField: String,
                                 prefix: String
                               ): String = {
    if (items.isEmpty) return s"$prefix"

    val formatted = items.zipWithIndex.map { case (item, index) =>
      val message = item.getOrElse(msgField, "")
      s"${index + 1}、$message"
    }.mkString(" ")
    s"$prefix $formatted"
  }

  private def formatExpectedArray(items: List[Map[String, String]]): String = {
    if (items.isEmpty) return "预期涨停消息"

    val formatted = items.zipWithIndex.map { case (item, index) =>
      val message = item.getOrElse("预期消息", "")
      val continuity = item.getOrElse("延续性", "")
      s"${index + 1}、$message（$continuity）"
    }.mkString(" ")
    s"预期涨停消息 $formatted"
  }

  // 格式化深度分析方法
  private def formatDepthAnalysis(items: List[String]): List[String] = {
    if (items.isEmpty) return List.empty

    items.zipWithIndex.map { case (analysis, index) =>
      s"深度分析${index + 1}：$analysis"
    }
  }

}


