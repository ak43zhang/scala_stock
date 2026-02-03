package sparktask.test

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.util.Try

object SparkStockInfoFormatter {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("StockInfoFormatter")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // 模拟数据
    val data = Seq(
      ("1", """{
        "股票名称": "显盈科技",
        "涨停时间": "2025-07-25",
        "板块消息": [{"板块": "消费电子", "板块刺激消息": ["欧盟充电接口新规生效"]},{"板块": "消费电", "板块刺激消息": ["欧盟充电接口新规生"]}],
        "概念消息": [{"概念": "Type-C", "概念刺激消息": ["Type-C渗透率突破80%"]}],
        "龙头股消息": [{"龙头股": "立讯精密", "龙头股刺激消息": ["拿下苹果200亿订单"]}],
        "消息": [
          {"影响消息": "公司获Type-C新专利", "最早出现时间": "2025-07-23"},
          {"影响消息": "半年报预增120%", "最早出现时间": "2025-07-24"},
          {"影响消息": "与华为签订供应协议", "最早出现时间": "2025-07-21"}
        ],
        "预期涨停消息": [
          {"预期消息": "8月新品发布会", "最早出现时间": "2025-07-28", "延续性": "是"},
          {"预期消息": "北美大客户订单落地", "最早出现时间": "2025-07-29", "延续性": "是"},
          {"预期消息": "行业标准升级", "最早出现时间": "2025-07-26", "延续性": "否"}
        ]
      }"""),("2","""{
                         |  "股票名称": "显盈科技",
                         |  "涨停时间": "2025-07-25",
                         |  "板块消息": [
                         |    {
                         |      "板块": "消费电子",
                         |      "板块刺激消息": [
                         |        "欧盟充电接口新规生效"
                         |      ]
                         |    }
                         |  ],
                         |  "概念消息": [
                         |    {
                         |      "概念": "Type-C",
                         |      "概念刺激消息": [
                         |        "Type-C渗透率突破80%"
                         |      ]
                         |    }
                         |  ],
                         |  "龙头股消息": [
                         |    {
                         |      "龙头股": "立讯精密",
                         |      "龙头股刺激消息": [
                         |        "拿下苹果200亿订单"
                         |      ]
                         |    }
                         |  ],
                         |  "消息": [
                         |    {
                         |      "影响消息": "公司获Type-C新专利",
                         |      "最早出现时间": "2025-07-23"
                         |    },
                         |    {
                         |      "影响消息": "半年报预增120%",
                         |      "最早出现时间": "2025-07-24"
                         |    },
                         |    {
                         |      "影响消息": "与华为签订供应协议",
                         |      "最早出现时间": "2025-07-21"
                         |    }
                         |  ],
                         |  "预期涨停消息": [
                         |    {
                         |      "预期消息": "8月新品发布会",
                         |      "最早出现时间": "2025-07-28",
                         |      "延续性": "是"
                         |    },
                         |    {
                         |      "预期消息": "北美大客户订单落地",
                         |      "最早出现时间": "2025-07-29",
                         |      "延续性": "是"
                         |    },
                         |    {
                         |      "预期消息": "行业标准升级",
                         |      "最早出现时间": "2025-07-26",
                         |      "延续性": "否"
                         |    }
                         |  ]
                         |}""".stripMargin)
    ).toDF("id", "json_column")

    val formatStockInfoUDF = udf(formatStockInfo _)

    data.createOrReplaceTempView("stock_data")

    // 注册UDF格式化函数
    spark.udf.register("format_stock_info", formatStockInfoUDF)

    // 应用UDF
    val result = data.selectExpr(
      "format_stock_info(json_column) as formatted_info"
    )

    result.show(truncate = false)

    spark.stop()
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

      // 拼接所有部分，只添加非空部分
      val resultBuilder = List.newBuilder[String]

      // 总是添加股票名称和涨停时间
      resultBuilder += s"股票名称: $stockName,"
      resultBuilder += s"涨停时间: $limitUpTime,"

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
}