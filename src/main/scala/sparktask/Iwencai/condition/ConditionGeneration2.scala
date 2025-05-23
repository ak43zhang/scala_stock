package sparktask.Iwencai.condition

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.{Date, Properties}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.math.BigDecimal.RoundingMode

/**
 * I问财条件拼接
 */
object ConditionGeneration2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions","10")
      .set("spark.driver.memory","8g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "10")
      .set("spark.local.dir","D:\\SparkTemp")

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    createTableView(spark)


//    val date = "2025-02-13"
    val date = "2025-01-02"
    midConditionGeneration2(spark,date)
    spark.close()
  }

  def createTableView(spark:SparkSession): Unit ={
    val url = "jdbc:mysql://localhost:3306/gs"
    val driver = "com.mysql.cj.jdbc.Driver"
    val user = "root"
    val pwd = "123456"

    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", pwd)
    properties.setProperty("url", url)
    properties.setProperty("driver", driver)

    val df:DataFrame = spark.read.jdbc(url, "data_jyrl", properties)
    df.createOrReplaceTempView("ta1")

    val zsdf:DataFrame = spark.read.jdbc(url, "data_zshq_ths", properties)
    zsdf.createOrReplaceTempView("ta2")

    val lhbdf:DataFrame = spark.read.jdbc(url, "data_lhb", properties)
    lhbdf.createOrReplaceTempView("ta3")

    val rzrqdf:DataFrame = spark.read.jdbc(url, "data_rzrq", properties)
    rzrqdf.createOrReplaceTempView("ta4")
  }

  /**
   * 中间层，用于和其他分支合并
   * @param spark
   * @param date
   */
  def midConditionGeneration2(spark:SparkSession,date:String): Unit ={

    println("当前条件生成日期为："+date)
    println("---------------")


    val dateArray = spark.sql(
      s"""select trade_date from ((select trade_date from ta1 where trade_status=1 and trade_date<='$date' order by trade_date desc limit 250)
         |union
         |(select trade_date from ta1 where trade_status=1 and trade_date>'$date' order by trade_date  limit 2))  order by trade_date desc limit 250""".stripMargin)
      .collect()
      .map(f=>f.getAs[String]("trade_date"))

    val ht = dateArray(0)
    val mt = dateArray(1)
    val jt = dateArray(2)
    val zt = dateArray(3)
    val qt = dateArray(4)
    val dqt =  dateArray(5)
    val oneMonthsAgo = dateArray(20)
    val twoMonthsAgo = dateArray(40)
    val threeMonthsAgo = dateArray(60)
    val halfYearAgo = dateArray(120)
    val oneYearAgo = dateArray(240)

    //    System.out.println(dqt)
        System.out.println(qt)
        System.out.println(zt)
    //    System.out.println(jt)
    //    System.out.println(mt)
    //    System.out.println(ht)
    //    System.out.println(twoMonthsAgo)

    val zs2dayres = zsfx(spark,qt,zt)
    lhbfx(spark,zt)
    rzrq(spark,zt)

    val ztzsbj = spark.sql(s"""select if(change_pct>=0,1,0) as ztzs,change_pct as zxpzs from ta2 where index_code ='399401' and trade_date='$zt' limit 10""")
      .collect().map(f=>(f.getAs[Int]("ztzs"),f.getAs[Double]("zxpzs")))
    val ztzsbjres = ztzsbj(0)._1
    val ztzxpzs = f"${ztzsbj(0)._2}%.2f".toFloat

    val qtzsbj = spark.sql(s"""select if(change_pct>=0,1,0) as qtzs,change_pct as zxpzs from ta2 where index_code ='399401' and trade_date='$qt' limit 10""")
      .collect().map(f=>(f.getAs[Int]("qtzs"),f.getAs[Double]("zxpzs")))
    val qtzsbjres = qtzsbj(0)._1
    val qtzxpzs = f"${qtzsbj(0)._2}%.2f".toFloat


    if(ztzxpzs>2){
      println("昨日中小盘涨幅超过百分之2，请注意早盘获利了结风险")
    }
    //四分之一分位，中位数，四分之三分位

    //火热板块

    //
    val ztzf: String  = if(ztzsbjres==1){
      s"${zt}涨跌幅小于${ztzxpzs+2}%，"
    }else{
      s"${zt}涨跌幅小于2%，"
    }

    val qtzf: String = if(qtzsbjres==1){
      s"${qt}涨跌幅小于${qtzxpzs+2}%，"
    }else{
      s"${qt}涨跌幅小于2%，"
    }

    //    val ztzf = if(ztzsbjres==1){
    //      s"${zt}涨跌幅小于4%，"
    //    }else{
    //      s"${zt}涨跌幅小于2%，"
    //    }
    //TODO 问财query查询条件生成，数字确定化

    //,${zt}有公告取反,${zt}最大涨幅
    //非科创，非北证 ，基金持股占流通股比例
    //原则2：普涨行情不设防【某天黄白线都红，则取消当天涨跌幅限制】
    //公司出质人质押占总股本比合计小于百分之25或无质押，
    val sqlString =
    s"""
       |${oneMonthsAgo}到${zt}换手率>40，${zt}实际换手率1到25，${twoMonthsAgo}到${zt}涨停次数大于0，非st，主板，上市交易天数>180,
       |总市值20亿到200亿，实际流通市值10亿到150亿，近1年被立案调查的股取反，未来2个月有解禁的股取反，未来两个月分红取反，
       |${ztzf}${qtzf}
       |""".stripMargin.replaceAll("-","").replaceAll("\r\n","")

    System.out.println(sqlString)

    val sqlString2 =
      s"""
        |${twoMonthsAgo}到${zt}涨停次数大于0,
        |公司出质人质押占总股本比合计小于百分之25或无质押，
        |总市值20亿到200亿,非st，主板，上市交易天数>180,
        |${ztzf}${qtzf}
        |机构持股占流通股比例<60%,机构持股家数<=10家
        |""".stripMargin.replaceAll("-","").replaceAll("\r\n","")

    System.out.println(sqlString2)

    //基本股票筛选条件
    // TODO 待确定 20250205缩量
    val zshb = BigDecimal(zs2dayres(0)._10+zs2dayres(0)._2).setScale(2, RoundingMode.HALF_UP).toDouble
    val sqlString3 =
      s"""
         |股价大于3元，主板，非st，${zt}总市值20亿到200亿，${zt}实际流通市值10亿到150亿，${zt}上市交易天数>180天,${zt}股价大于3元,
         |${twoMonthsAgo}到${zt}涨停次数大于0,
         |,${qt}到${zt}区间涨跌幅<=${zshb}
         |""".stripMargin.replaceAll("-","").replaceAll("\r\n","")

    System.out.println(sqlString3)

    import spark.implicits._
    //currentdate:当前时间，用于获取当天需要执行的所有数据条件    date:执行某天的时间
    val currentdate = new SimpleDateFormat("yyyyMMdd").format(new Date())
    spark.sparkContext.makeRDD(Seq((currentdate,date,"sqlString",sqlString),(currentdate,date,"sqlString2",sqlString2)))
      .toDF("currentdate","date","conditiontype","sqlString2").show(false)
  }

  /**
   * 指数分析
   * @param spark
   * @param qt
   * @param zt
   */
  def zsfx(spark:SparkSession,qt:String,zt:String): Array[(Int, Double, String, String, String, String, String, String, String, Double)] ={
    /*
    指数情绪总共分为四种
      高开低走（弱情绪）
      红柱（强情绪延续）
      绿柱（弱情绪延续）
      低开高走（强情绪）
     */
    val zs2daydf = spark.sql(
      s"""
         |select t1.change_pct as qtzf,t2.*,
         |if(t1.close>t2.open,'低开',if(t1.close=t2.open,'平开','高开')) as kpzf,
         |if(t2.close>t2.open,'高走',if(t2.close=t2.open,'平走','低走')) as spzf,
         |if(t2.volume>t1.volume,'放量','缩量') as ln,
         |if(t2.amount>t1.amount,'放量','缩量') as dn,
         |if(t2.high>t1.high,'上升','包含') as sfss,
         |if(t2.low<t1.low,'下探','包含') as sfxt,
         |case when t2.open<t2.close and (t2.close-t2.open>=t2.high-t2.close+t2.open-t2.low or (t2.close-t2.open>=t2.high-t2.close and t2.close-t2.open>=t2.open-t2.low)) then '红柱'
         |	 when t2.open<t2.close and (t2.high-t2.close>=t2.close-t2.low or (t2.high-t2.close>=t2.close-t2.open and t2.high-t2.close>=t2.open-t2.low and t2.close-t2.open>=t2.open-t2.low)) then '红柱上影线'
         |	 when t2.open<t2.close and (t2.open-t2.low>=t2.high-t2.open or (t2.open-t2.low>=t2.close-t2.open and t2.open-t2.low>=t2.high-t2.close and t2.close-t2.open>=t2.high-t2.close)) then '红柱下影线'
         |	 when (t2.open<t2.close or (t2.open=t2.close and t2.open>t1.close)) and t2.high-t2.close>=t2.close-t2.open and t2.open-t2.low>=t2.close-t2.open and t2.high!=t2.low then '红柱十字星'
         |	 when t2.close<t2.open and (t2.open-t2.close>=t2.high-t2.open+t2.close-t2.low or (t2.open-t2.close>=t2.high-t2.open and t2.open-t2.close>=t2.close-t2.low)) then '绿柱'
         |	 when t2.close<t2.open and (t2.high-t2.open>=t2.open-t2.low or (t2.high-t2.open>=t2.close-t2.low and t2.high-t2.open>=t2.open-t2.close and t2.open-t2.close>=t2.close-t2.low)) then '绿柱上影线'
         |	 when t2.close<t2.open and (t2.close-t2.low>=t2.high-t2.close or (t2.close-t2.low>=t2.high-t2.open and t2.close-t2.low>=t2.open-t2.close and t2.open-t2.close>=t2.high-t2.open))  then '绿柱下影线'
         |	 when (t2.open>t2.close or (t2.open=t2.close and t2.open<t1.close)) and t2.high-t2.open>=t2.open-t2.close and t2.close-t2.low>=t2.open-t2.close and t2.high!=t2.low  then '绿柱十字星'
         |	 when t2.high=t2.open and t2.open=t2.close and t2.close=t2.low and t2.change_pct>9 then '涨停一字'
         |	 when t2.high=t2.open and t2.open=t2.close and t2.close=t2.low and t2.change_pct<-9 then '跌停一字'
         |	else '其他'
         |end as kxzt
         | from (select *,if(change_pct>0,1,0) as qtzs,change_pct as zxpzs from ta2 where index_code ='399401' and trade_date='${qt}') as t1
         |left join
         |(select *,if(change_pct>0,1,0) as ztzs,change_pct as zxpzs from ta2 where index_code ='399401' and trade_date='${zt}') as t2
         |on t1.index_code=t2.index_code
         |""".stripMargin)

    zs2daydf.show()

    val zs2dayres: Array[(Int, Double, String, String, String, String, String, String, String, Double)] = zs2daydf.collect().map(f =>
      (f.getAs[Int]("ztzs"), //
        f.getAs[Double]("zxpzs"), //中小盘指数
        f.getAs[String]("kpzf"), //开盘涨幅
        f.getAs[String]("spzf"), //收盘涨幅
        f.getAs[String]("ln"), //量能
        f.getAs[String]("dn"), //动能
        f.getAs[String]("sfss"), //是否上升
        f.getAs[String]("sfxt"), //是否下调
        f.getAs[String]("kxzt"), //k线状态
        f.getAs[String]("qtzf").asInstanceOf[Double] //前天涨幅
      ))
    //TODO 根据前天的数据分析昨日数据走势，做出预测
    println("-----------------------------中小盘指数分析-------------------------------------")
    println("前天中小盘指数涨幅："+zs2dayres(0)._10+"\n"+"昨天中小盘指数涨幅："+zs2dayres(0)._2)
    //走势判断：高开高走，高开低走，低开高走，低开低走
    println("昨日走势:"+zs2dayres(0)._3+zs2dayres(0)._4)
    println("与上一天比较：最高点"+zs2dayres(0)._7+"，最低点"+zs2dayres(0)._8)
    println("量能能力："+zs2dayres(0)._5)
    println("动能能力："+zs2dayres(0)._6)
    println("k线状态："+zs2dayres(0)._9)
    println("---------------------------------解释------------------------------------")
    //TODO 字典分别对应各种情况
    println("1.对于走势：低走代表情绪走弱，高开高走小心次日利好兑现，低开高走情绪走强")
    println("2.对于量能，根据走势状态分析量能，一般缩量都为不强势，高开低走放量=利好兑现，高开高走放量=市场处于强势位置")
    println("3.对于动能，根据走势状态分析动能，一般缩量都为参与资金变少，高开低走放量=利好兑现，高开高走放量=市场处于强势位置")
    println("4.对于高低点判断，根据k线状态进行分析，包含为延续走势")
    zs2dayres
  }

  /**
   * 龙虎榜分析
   */
  def lhbfx(spark:SparkSession,zt:String): Unit ={
    println("--------------------龙虎榜分析-----------------------")
    val df = spark.sql(
      s"""
        |select count(1) as `总数`,
        |sum(if(change_cpt>0,1,0)) as `上涨数量`,sum(if(change_cpt<0,1,0)) as `下跌数量`,
        |sum(if(change_cpt>9,1,0)) as `大于9`,sum(if(change_cpt<-9,1,0)) as `小于-9`
        |from ta3 where trade_date='${zt}'
        | and (stock_code like '00%' or stock_code like '60%')
        |""".stripMargin)

    //TODO 共多少条，有多少股上榜，红的多少，绿的多少，流入最多的前5个股，流出最多的前5个股【只看中小盘】
    df.show()
  }

  /**
   * 板块分析
   */
  def bkfx(): Unit ={

  }

  /**
   * 个股分析
   */
  def ggfx(): Unit ={

  }

  /**
   * 热股分析
   */
  def rdfx(): Unit ={

  }

  /**
   * 融资融券
   */
  def rzrq(spark:SparkSession,zt:String): Unit ={
    println("--------------------融资融券-----------------------")
    spark.sql(
      s"""
        |select * from ta4 where trade_date='$zt' limit 1
        |""".stripMargin).show()
  }

  /**
   * 总市值计算
   */
  def zsz(spark:SparkSession,zt:String): Unit ={
    println("--------------------总市值-----------------------")
    spark.sql(
      s"""
         |select * from ta4 where trade_date='$zt' limit 1
         |""".stripMargin).show()
  }

}
