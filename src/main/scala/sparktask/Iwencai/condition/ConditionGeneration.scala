package sparktask.Iwencai.condition

import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * I问财条件拼接
 */
object ConditionGeneration {
  def main(args: Array[String]): Unit = {

    val startDate = LocalDate.of(2024, 1, 1)
    val endDate = LocalDate.of(2025, 1, 2)//如果需要明天的数据，则需要定义明天以后得一个工作日期
    val oneMonthsAgo = endDate.minusMonths(1)
//    val endDate = LocalDate.now.minusDays(-1)//结果当天
    val tradingDays = getTradingDays(startDate, endDate)
    val lastFive = tradingDays.drop(tradingDays.length - 5).take(5)
    println("-----------")
    lastFive.foreach(println)
    println("-----------")
    val jgt = lastFive(4)
    val jt = lastFive(3)
    val zt = lastFive(2)
    val qt = lastFive(1)
    val dqt = lastFive.head

    System.out.println(dqt)
    System.out.println(qt)
    System.out.println(zt)
    System.out.println(jt)
    System.out.println(jgt)
    System.out.println(oneMonthsAgo)

    getChineseDay(jgt)

    //非科创，非北证
    val sqlString =
      s"""
         |${oneMonthsAgo}到${zt}涨停次数大于0，非st，主板，上市交易天数>60,
         |实际流通市值小于200亿，近1年被立案调查的股取反，未来2个月有解禁的股取反，未来两个月分红取反，
         |${qt}涨跌幅小于2%，
         |${zt}涨跌幅小于2%，
         |${zt}缩量，
         |${zt}人气排3000名以内，
         |${jt}竞价涨跌幅在-2到2之间，
         |${jt} 9:30分时主力流向在500万到-500万之间，${jt}最高涨幅大于3，
         |（${jgt}集合竞价价格-${zt}收盘价*1.03）/${jt}收盘价
         |""".stripMargin

    System.out.println(sqlString)

  }


  /**
   * 条件1
   * 锚定打野流
   * 某天异常上涨，涨幅或者最大涨幅大于百分之5，
   * 连续2-10天区间跌幅是涨幅的75%以上
   */
  def condition1(): String ={

    """近两个月涨停次数大于0，非st，非科创，非北证，上市交易天数>60,
      实际流通市值小于200亿，近1年被立案调查的股取反，未来2个月有解禁的股取反，未来两个月分红取反
      ,2024-11-25最大涨幅超过百分之6，2024-11-26到2024-11-28区间跌幅""".stripMargin

  }

  /**
   * 由yyyy-MM-dd转换成yyyy年M月d日
   */
  def getChineseDay(mt:LocalDate):String = {
    val inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val outputFormatter = DateTimeFormatter.ofPattern("yyyy年M月d日")

    val localDate_mt = LocalDate.parse(mt.toString, inputFormatter)
    val newDateStr_mt = localDate_mt.format(outputFormatter)
    System.out.println(newDateStr_mt)
    newDateStr_mt
  }

  /**
   * 获取近5个交易日
   * 分别是 -3，-2，-1，0,1
   * 如果要明天的数据，则endDate设置为明天
   * @param startDate
   * @param endDate
   * @return
   */
  def getTradingDays(startDate: LocalDate, endDate: LocalDate): List[LocalDate] = {
    var currentDate = startDate
    val tradingDaysList = scala.collection.mutable.ListBuffer[LocalDate]()
    while (currentDate.isBefore(endDate) || currentDate.isEqual(endDate)) {
      if (currentDate.getDayOfWeek.getValue <= 5) {
        tradingDaysList += currentDate
      }
      currentDate = currentDate.plusDays(1)
    }
    tradingDaysList.toList
  }

}
