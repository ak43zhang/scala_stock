package sparktask.mysql.update

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import sparktask.mysql.update.Update15days.all15days

import scala.collection.mutable.ArrayBuffer

/**
 * 更新数据
 */
object UpdateDate2Parquet {
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
      .appName("UpdateDate2Parquet")
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
    properties.setProperty("partitionColumn", "amount") // 选择一个分区列
    properties.setProperty("lowerBound", "10000") // 分区列的最小值
    properties.setProperty("upperBound", "1000000") // 分区列的最大值
    properties.setProperty("numPartitions", "8") // 设置分区数量

    val jyrlsAll = ArrayBuffer("data2024_gpsj_day","data2024_gpsj_day_0318","data2024_gpsj_day_0319","data2024_gpsj_day_0320","data2024_gpsj_day_0321","data2024_gpsj_day_0325","data2024_gpsj_day_0326","data2024_gpsj_day_0327","data2024_gpsj_day_0328","data2024_gpsj_day_0329","data2024_gpsj_day_0401","data2024_gpsj_day_0402","data2024_gpsj_day_0403","data2024_gpsj_day_0414","data2024_gpsj_day_0514","data2024_gpsj_day_0702","data2024_gpsj_day_0704","data2024_gpsj_day_0705","data2024_gpsj_day_0709","data2024_gpsj_day_0710","data2024_gpsj_day_0711","data2024_gpsj_day_0712","data2024_gpsj_day_0715","data2024_gpsj_day_0716","data2024_gpsj_day_0717","data2024_gpsj_day_0718","data2024_gpsj_day_0719"
      ,"data2024_gpsj_day_0722","data2024_gpsj_day_0723","data2024_gpsj_day_0724","data2024_gpsj_day_0725","data2024_gpsj_day_0726")
    val jyrlsSome = ArrayBuffer("data2024_gpsj_day_0729")
    import spark.implicits._

//    updateQ9H6V2Test(spark)
    /**
     * 全量股票数据更新
     */
//    updateGpsj(spark,url,jyrlsAll,properties)
    /**
     * 每日股票数据更新
     */
//    updateGpsj(spark,url,jyrlsSome,properties)
    /**
     * 更新前9后6大宽表
     */
//    val start = System.currentTimeMillis()
//    updateQ9H6V2(spark)
//    val end = System.currentTimeMillis()
//    print("共耗时："+(end-start)/1000+"秒")

    /**
     * 按月更新前9后6大宽表
     */
    val startm = System.currentTimeMillis()
    val months = "2015-02,2015-03,2015-04"
    updateQ9H6forMonth(spark,months)
    val endm = System.currentTimeMillis()
    print("共耗时："+(endm-startm)/1000+"秒")


    spark.close()
  }

  /**
   * 按天更新股票数据
   *
   */
  def updateGpsj(spark: SparkSession,url:String,jyrls:ArrayBuffer[String],properties: Properties): Unit = {
    var df:DataFrame = spark.read.jdbc(url, jyrls(0), properties)
    val startm = System.currentTimeMillis()
    if(jyrls.size>1){
      for(i<-1 until jyrls.size){
        println("准备处理："+jyrls(i))
        df = df.union(spark.read.jdbc(url, jyrls(i), properties))
        println("处理完毕："+jyrls(i))
      }
    }

    df.createOrReplaceTempView("ta1")
    //按月保存每日数据
    //TODO 竞价亏赚钱,阶段高点与阶段低点
    //TODO open,high,low,close分别对应涨跌幅
    spark.sql("select *,if(close>=round(pre_close*1.1,2),1,0) as sfzt,substring(trade_date,0,7) as trade_date_month from ta1  order by stock_code")
      .drop("index")
      .distinct().repartition(1).write.mode("append").partitionBy("trade_date_month").parquet("file:///D:\\gsdata\\gpsj_day")
    val endm = System.currentTimeMillis()
    println("共耗时："+(endm-startm)/1000+"秒")
  }

  /**
   * 更新前9后6大宽表
   */
  def updateQ9H6V2(spark: SparkSession): Unit = {
    val startm = System.currentTimeMillis()
    val df2 = spark.read.parquet("file:///D:\\gsdata\\gpsj_day")
      .withColumn("open", col("open").cast(DecimalType(10, 2)))
      .withColumn("close", col("close").cast(DecimalType(10, 2)))
      .withColumn("volume", col("volume").cast(IntegerType))
      .withColumn("high", col("high").cast(DecimalType(10, 2)))
      .withColumn("low", col("low").cast(DecimalType(10, 2)))
      .withColumn("amount", col("amount").cast(IntegerType))
      .withColumn("change", col("change").cast(DecimalType(10, 2)))
      .withColumn("change_pct", col("change_pct").cast(DecimalType(10, 2)))
      .withColumn("pre_close", col("pre_close").cast(DecimalType(10, 2)))

    df2.cache()
    df2.createTempView("ta2")

    val df3 = spark.sql("select *,row_number() over(partition by stock_code order by trade_date,stock_code) as row from ta2 ")
    df3.cache()
    df3.createTempView("ta3")
    //TODO 连板数
    val df4 = spark.sql(
      """
        |select t2.*,
        |if(t2.open<t1.close,'低开',if(t2.open=t1.close,'平开','高开')) as kp,
        |if(t2.open<t2.close,'高走',if(t2.open=t1.close,'平走','低走')) as wp,
        |if(t2.high<t1.high,'下探',if(t2.high=t1.high,'平整','突破')) as sx,
        |if(t2.low<t1.low,'下探',if(t2.low=t1.low,'平整','突破')) as xx,
        |if(t2.volume<t1.volume,'缩量',if(t2.volume=t1.volume,'平量',if(t2.volume/t1.volume>2,'放巨量','放量'))) as ln,
        |round(t2.volume/t1.volume,2) as zrlnb,
        |case when t2.open<t2.close and (t2.close-t2.open>=t2.high-t2.close+t2.open-t2.low or (t2.close-t2.open>=t2.high-t2.close and t2.close-t2.open>=t2.open-t2.low)) then '红柱'
        |	 when t2.open<t2.close and (t2.high-t2.close>=t2.close-t2.low or (t2.high-t2.close>=t2.close-t2.open and t2.high-t2.close>=t2.open-t2.low and t2.close-t2.open>=t2.open-t2.low)) then '红柱上影线'
        |	 when t2.open<t2.close and (t2.open-t2.low>=t2.high-t2.open or (t2.open-t2.low>=t2.close-t2.open and t2.open-t2.low>=t2.high-t2.close and t2.close-t2.open>=t2.high-t2.close)) then '红柱下影线'
        |	 when (t2.open<t2.close or (t2.open=t2.close and t2.open>t2.pre_close)) and t2.high-t2.close>=t2.close-t2.open and t2.open-t2.low>=t2.close-t2.open and t2.high!=t2.low then '红柱十字星'
        |	 when t2.close<t2.open and (t2.open-t2.close>=t2.high-t2.open+t2.close-t2.low or (t2.open-t2.close>=t2.high-t2.open and t2.open-t2.close>=t2.close-t2.low)) then '绿柱'
        |	 when t2.close<t2.open and (t2.high-t2.open>=t2.open-t2.low or (t2.high-t2.open>=t2.close-t2.low and t2.high-t2.open>=t2.open-t2.close and t2.open-t2.close>=t2.close-t2.low)) then '绿柱上影线'
        |	 when t2.close<t2.open and (t2.close-t2.low>=t2.high-t2.close or (t2.close-t2.low>=t2.high-t2.open and t2.close-t2.low>=t2.open-t2.close and t2.open-t2.close>=t2.high-t2.open))  then '绿柱下影线'
        |	 when (t2.open>t2.close or (t2.open=t2.close and t2.open<t2.pre_close)) and t2.high-t2.open>=t2.open-t2.close and t2.close-t2.low>=t2.open-t2.close and t2.high!=t2.low  then '绿柱十字星'
        |	 when t2.high=t2.open and t2.open=t2.close and t2.close=t2.low and t2.change_pct>9 then '涨停一字'
        |	 when t2.high=t2.open and t2.open=t2.close and t2.close=t2.low and t2.change_pct<-9 then '跌停一字'
        |	else '其他'
        |end as zt
        |from ta3 as t1 left join ta3 as t2 on t1.row+1=t2.row and t1.stock_code=t2.stock_code
        |where t2.row is not null and t2.stock_code like '00%' or t2.stock_code like '60%'""".stripMargin)
    df4.cache()
    df4.createTempView("ta4")

    spark.sql(
      """|select t0.stock_code,substring(t0.trade_date,0,7) as trade_date_month,
         |t0.trade_date as t0_trade_date,t0.sfzt as t0_sfzt, t0.open as t0_open , t0.close as t0_close ,t0.high as t0_high,t0.low as t0_low,t0.volume as t0_volume,t0.amount as t0_amount,t0.pre_close as t0_pre_close,t0.kp as t0_kp,t0.wp as t0_wp ,t0.sx as t0_sx,t0.xx as t0_xx,t0.ln as t0_ln,t0.zrlnb as t0_zrlnb,t0.zt as t0_zt,t0.turnover_ratio as t0_turnover_ratio,t0.change as t0_change,t0.change_pct as t0_change_pct,
         |t1.trade_date as t1_trade_date,t1.sfzt as t1_sfzt, t1.open as t1_open , t1.close as t1_close ,t1.high as t1_high,t1.low as t1_low,t1.volume as t1_volume,t1.amount as t1_amount,t1.pre_close as t1_pre_close,t1.kp as t1_kp,t1.wp as t1_wp ,t1.sx as t1_sx,t1.xx as t1_xx,t1.ln as t1_ln,t1.zrlnb as t1_zrlnb,t1.zt as t1_zt,t1.turnover_ratio as t1_turnover_ratio,t1.change as t1_change,t1.change_pct as t1_change_pct,
         |t2.trade_date as t2_trade_date,t2.sfzt as t2_sfzt, t2.open as t2_open , t2.close as t2_close ,t2.high as t2_high,t2.low as t2_low,t2.volume as t2_volume,t2.amount as t2_amount,t2.pre_close as t2_pre_close,t2.kp as t2_kp,t2.wp as t2_wp ,t2.sx as t2_sx,t2.xx as t2_xx,t2.ln as t2_ln,t2.zrlnb as t2_zrlnb,t2.zt as t2_zt,t2.turnover_ratio as t2_turnover_ratio,t2.change as t2_change,t2.change_pct as t2_change_pct,
         |t3.trade_date as t3_trade_date,t3.sfzt as t3_sfzt, t3.open as t3_open , t3.close as t3_close ,t3.high as t3_high,t3.low as t3_low,t3.volume as t3_volume,t3.amount as t3_amount,t3.pre_close as t3_pre_close,t3.kp as t3_kp,t3.wp as t3_wp ,t3.sx as t3_sx,t3.xx as t3_xx,t3.ln as t3_ln,t3.zrlnb as t3_zrlnb,t3.zt as t3_zt,t3.turnover_ratio as t3_turnover_ratio,t3.change as t3_change,t3.change_pct as t3_change_pct,
         |t4.trade_date as t4_trade_date,t4.sfzt as t4_sfzt, t4.open as t4_open , t4.close as t4_close ,t4.high as t4_high,t4.low as t4_low,t4.volume as t4_volume,t4.amount as t4_amount,t4.pre_close as t4_pre_close,t4.kp as t4_kp,t4.wp as t4_wp ,t4.sx as t4_sx,t4.xx as t4_xx,t4.ln as t4_ln,t4.zrlnb as t4_zrlnb,t4.zt as t4_zt,t4.turnover_ratio as t4_turnover_ratio,t4.change as t4_change,t4.change_pct as t4_change_pct,
         |t5.trade_date as t5_trade_date,t5.sfzt as t5_sfzt, t5.open as t5_open , t5.close as t5_close ,t5.high as t5_high,t5.low as t5_low,t5.volume as t5_volume,t5.amount as t5_amount,t5.pre_close as t5_pre_close,t5.kp as t5_kp,t5.wp as t5_wp ,t5.sx as t5_sx,t5.xx as t5_xx,t5.ln as t5_ln,t5.zrlnb as t5_zrlnb,t5.zt as t5_zt,t5.turnover_ratio as t5_turnover_ratio,t5.change as t5_change,t5.change_pct as t5_change_pct,
         |t6.trade_date as t6_trade_date,t6.sfzt as t6_sfzt, t6.open as t6_open , t6.close as t6_close ,t6.high as t6_high,t6.low as t6_low,t6.volume as t6_volume,t6.amount as t6_amount,t6.pre_close as t6_pre_close,t6.kp as t6_kp,t6.wp as t6_wp ,t6.sx as t6_sx,t6.xx as t6_xx,t6.ln as t6_ln,t6.zrlnb as t6_zrlnb,t6.zt as t6_zt,t6.turnover_ratio as t6_turnover_ratio,t6.change as t6_change,t6.change_pct as t6_change_pct,
         |t7.trade_date as t7_trade_date,t7.sfzt as t7_sfzt, t7.open as t7_open , t7.close as t7_close ,t7.high as t7_high,t7.low as t7_low,t7.volume as t7_volume,t7.amount as t7_amount,t7.pre_close as t7_pre_close,t7.kp as t7_kp,t7.wp as t7_wp ,t7.sx as t7_sx,t7.xx as t7_xx,t7.ln as t7_ln,t7.zrlnb as t7_zrlnb,t7.zt as t7_zt,t7.turnover_ratio as t7_turnover_ratio,t7.change as t7_change,t7.change_pct as t7_change_pct,
         |t8.trade_date as t8_trade_date,t8.sfzt as t8_sfzt, t8.open as t8_open , t8.close as t8_close ,t8.high as t8_high,t8.low as t8_low,t8.volume as t8_volume,t8.amount as t8_amount,t8.pre_close as t8_pre_close,t8.kp as t8_kp,t8.wp as t8_wp ,t8.sx as t8_sx,t8.xx as t8_xx,t8.ln as t8_ln,t8.zrlnb as t8_zrlnb,t8.zt as t8_zt,t8.turnover_ratio as t8_turnover_ratio,t8.change as t8_change,t8.change_pct as t8_change_pct

         | from ta4 as t0
         | left join ta4 as t1 on t0.row+1=t1.row and t0.stock_code=t1.stock_code
         | left join ta4 as t2 on t0.row+2=t2.row and t0.stock_code=t2.stock_code
         | left join ta4 as t3 on t0.row+3=t3.row and t0.stock_code=t3.stock_code
         | left join ta4 as t4 on t0.row+4=t4.row and t0.stock_code=t4.stock_code
         | left join ta4 as t5 on t0.row+5=t5.row and t0.stock_code=t5.stock_code
         | left join ta4 as t6 on t0.row+6=t6.row and t0.stock_code=t6.stock_code
         | left join ta4 as t7 on t0.row+7=t7.row and t0.stock_code=t7.stock_code
         | left join ta4 as t8 on t0.row+8=t8.row and t0.stock_code=t8.stock_code

         |""".stripMargin).createTempView("ta5")

    spark.sql("select * from ta5")
      .repartition(8).write.mode("overwrite").partitionBy("trade_date_month").parquet("file:///D:\\gsdata\\gpsj_9days")

    spark.sql(
      """|select t0.stock_code,substring(t0.trade_date,0,7) as trade_date_month,
         |t0.trade_date as t0_trade_date,
         | t9.trade_date as  t9_trade_date, t9.sfzt as  t9_sfzt,  t9.open as  t9_open ,  t9.close as  t9_close , t9.high as  t9_high, t9.low as  t9_low, t9.volume as  t9_volume, t9.amount as  t9_amount, t9.pre_close as   t9_pre_close, t9.kp as  t9_kp, t9.wp as  t9_wp , t9.sx as  t9_sx, t9.xx as  t9_xx, t9.ln as  t9_ln, t9.zrlnb as  t9_zrlnb, t9.zt as  t9_zt, t9.turnover_ratio as  t9_turnover_ratio, t9.change as  t9_change, t9.change_pct as  t9_change_pct,
         |t10.trade_date as t10_trade_date,t10.sfzt as t10_sfzt, t10.open as t10_open , t10.close as t10_close ,t10.high as t10_high,t10.low as t10_low,t10.volume as t10_volume,t10.amount as t10_amount,t10.pre_close as  t10_pre_close,t10.kp as t10_kp,t10.wp as t10_wp ,t10.sx as t10_sx,t10.xx as t10_xx,t10.ln as t10_ln,t10.zrlnb as t10_zrlnb,t10.zt as t10_zt,t10.turnover_ratio as t10_turnover_ratio,t10.change as t10_change,t10.change_pct as t10_change_pct,
         |t11.trade_date as t11_trade_date,t11.sfzt as t11_sfzt, t11.open as t11_open , t11.close as t11_close ,t11.high as t11_high,t11.low as t11_low,t11.volume as t11_volume,t11.amount as t11_amount,t11.pre_close as  t11_pre_close,t11.kp as t11_kp,t11.wp as t11_wp ,t11.sx as t11_sx,t11.xx as t11_xx,t11.ln as t11_ln,t11.zrlnb as t11_zrlnb,t11.zt as t11_zt,t11.turnover_ratio as t11_turnover_ratio,t11.change as t11_change,t11.change_pct as t11_change_pct,
         |t12.trade_date as t12_trade_date,t12.sfzt as t12_sfzt, t12.open as t12_open , t12.close as t12_close ,t12.high as t12_high,t12.low as t12_low,t12.volume as t12_volume,t12.amount as t12_amount,t12.pre_close as  t12_pre_close,t12.kp as t12_kp,t12.wp as t12_wp ,t12.sx as t12_sx,t12.xx as t12_xx,t12.ln as t12_ln,t12.zrlnb as t12_zrlnb,t12.zt as t12_zt,t12.turnover_ratio as t12_turnover_ratio,t12.change as t12_change,t12.change_pct as t12_change_pct,
         |t13.trade_date as t13_trade_date,t13.sfzt as t13_sfzt, t13.open as t13_open , t13.close as t13_close ,t13.high as t13_high,t13.low as t13_low,t13.volume as t13_volume,t13.amount as t13_amount,t13.pre_close as  t13_pre_close,t13.kp as t13_kp,t13.wp as t13_wp ,t13.sx as t13_sx,t13.xx as t13_xx,t13.ln as t13_ln,t13.zrlnb as t13_zrlnb,t13.zt as t13_zt,t13.turnover_ratio as t13_turnover_ratio,t13.change as t13_change,t13.change_pct as t13_change_pct,
         |t14.trade_date as t14_trade_date,t14.sfzt as t14_sfzt, t14.open as t14_open , t14.close as t14_close ,t14.high as t14_high,t14.low as t14_low,t14.volume as t14_volume,t14.amount as t14_amount,t14.pre_close as  t14_pre_close,t14.kp as t14_kp,t14.wp as t14_wp ,t14.sx as t14_sx,t14.xx as t14_xx,t14.ln as t14_ln,t14.zrlnb as t14_zrlnb,t14.zt as t14_zt,t14.turnover_ratio as t14_turnover_ratio,t14.change as t14_change,t14.change_pct as t14_change_pct
         | from ta4 as t0
         | left join ta4 as  t9 on t0.row+9 = t9.row and t0.stock_code= t9.stock_code
         | left join ta4 as t10 on t0.row+10=t10.row and t0.stock_code=t10.stock_code
         | left join ta4 as t11 on t0.row+11=t11.row and t0.stock_code=t11.stock_code
         | left join ta4 as t12 on t0.row+12=t12.row and t0.stock_code=t12.stock_code
         | left join ta4 as t13 on t0.row+13=t13.row and t0.stock_code=t13.stock_code
         | left join ta4 as t14 on t0.row+14=t14.row and t0.stock_code=t14.stock_code

         |""".stripMargin).createTempView("ta6")

    spark.sql("select * from ta6")
      .repartition(8).write.mode("overwrite").partitionBy("trade_date_month").parquet("file:///D:\\gsdata\\gpsj_h6days")

    df2.unpersist()
    df3.unpersist()
    df4.unpersist()

    val endm = System.currentTimeMillis()
    print("共耗时："+(endm-startm)/1000+"秒")
  }

  /**
   * 更新前9后6大宽表
   */
  def updateQ9H6V2Test(spark: SparkSession): Unit = {
    val startm = System.currentTimeMillis()
    val df2 = spark.read.parquet("file:///D:\\gsdata\\gpsj_day\\trade_date_month=2024-0*").where("stock_code=600686")
      .withColumn("open", col("open").cast(DecimalType(10, 2)))
      .withColumn("close", col("close").cast(DecimalType(10, 2)))
      .withColumn("volume", col("volume").cast(IntegerType))
      .withColumn("high", col("high").cast(DecimalType(10, 2)))
      .withColumn("low", col("low").cast(DecimalType(10, 2)))
      .withColumn("amount", col("amount").cast(IntegerType))
      .withColumn("change", col("change").cast(DecimalType(10, 2)))
      .withColumn("change_pct", col("change_pct").cast(DecimalType(10, 2)))
      .withColumn("pre_close", col("pre_close").cast(DecimalType(10, 2)))

    df2.cache()
    df2.createTempView("ta2")

    val df3 = spark.sql("select *,row_number() over(partition by stock_code order by trade_date,stock_code) as row from ta2 ")
    df3.cache()
    df3.createTempView("ta3")
    //TODO 连板数
    val df4 = spark.sql(
      """
        |select t2.*,
        |if(t2.open<t1.close,'低开',if(t2.open=t1.close,'平开','高开')) as kp,
        |if(t2.open<t2.close,'高走',if(t2.open=t1.close,'平走','低走')) as wp,
        |if(t2.high<t1.high,'下探',if(t2.high=t1.high,'平整','突破')) as sx,
        |if(t2.low<t1.low,'下探',if(t2.low=t1.low,'平整','突破')) as xx,
        |if(t2.volume<t1.volume,'缩量',if(t2.volume=t1.volume,'平量',if(t2.volume/t1.volume>2,'放巨量','放量'))) as ln,
        |round(t2.volume/t1.volume,2) as zrlnb,
        |case when t2.open<t2.close and (t2.close-t2.open>=t2.high-t2.close+t2.open-t2.low or (t2.close-t2.open>=t2.high-t2.close and t2.close-t2.open>=t2.open-t2.low)) then '红柱'
        |	 when t2.open<t2.close and (t2.high-t2.close>=t2.close-t2.low or (t2.high-t2.close>=t2.close-t2.open and t2.high-t2.close>=t2.open-t2.low and t2.close-t2.open>=t2.open-t2.low)) then '红柱上影线'
        |	 when t2.open<t2.close and (t2.open-t2.low>=t2.high-t2.open or (t2.open-t2.low>=t2.close-t2.open and t2.open-t2.low>=t2.high-t2.close and t2.close-t2.open>=t2.high-t2.close)) then '红柱下影线'
        |	 when (t2.open<t2.close or (t2.open=t2.close and t2.open>t2.pre_close)) and t2.high-t2.close>=t2.close-t2.open and t2.open-t2.low>=t2.close-t2.open and t2.high!=t2.low then '红柱十字星'
        |	 when t2.close<t2.open and (t2.open-t2.close>=t2.high-t2.open+t2.close-t2.low or (t2.open-t2.close>=t2.high-t2.open and t2.open-t2.close>=t2.close-t2.low)) then '绿柱'
        |	 when t2.close<t2.open and (t2.high-t2.open>=t2.open-t2.low or (t2.high-t2.open>=t2.close-t2.low and t2.high-t2.open>=t2.open-t2.close and t2.open-t2.close>=t2.close-t2.low)) then '绿柱上影线'
        |	 when t2.close<t2.open and (t2.close-t2.low>=t2.high-t2.close or (t2.close-t2.low>=t2.high-t2.open and t2.close-t2.low>=t2.open-t2.close and t2.open-t2.close>=t2.high-t2.open))  then '绿柱下影线'
        |	 when (t2.open>t2.close or (t2.open=t2.close and t2.open<t2.pre_close)) and t2.high-t2.open>=t2.open-t2.close and t2.close-t2.low>=t2.open-t2.close and t2.high!=t2.low  then '绿柱十字星'
        |	 when t2.high=t2.open and t2.open=t2.close and t2.close=t2.low and t2.change_pct>9 then '涨停一字'
        |	 when t2.high=t2.open and t2.open=t2.close and t2.close=t2.low and t2.change_pct<-9 then '跌停一字'
        |	else '其他'
        |end as zt
        |from ta3 as t1 left join ta3 as t2 on t1.row+1=t2.row and t1.stock_code=t2.stock_code
        |where t2.row is not null and t2.stock_code like '00%' or t2.stock_code like '60%'""".stripMargin)
    df4.cache()
    df4.createTempView("ta4")

    df4.show(50,false)

    df2.unpersist()
    df3.unpersist()
    df4.unpersist()

    val endm = System.currentTimeMillis()
    print("共耗时："+(endm-startm)/1000+"秒")
  }

  def updateQ9H6forMonth(spark: SparkSession,months:String): Unit = {
    val beforeMonth =months.split(",")(0)
    val midMonth = months.split(",")(1)
    val nowMonth = months.split(",")(2)
    val df2 = spark.read.parquet(s"file:///D:\\gsdata\\gpsj_day\\trade_date_month=$beforeMonth",s"file:///D:\\gsdata\\gpsj_day\\trade_date_month=$midMonth",s"file:///D:\\gsdata\\gpsj_day\\trade_date_month=$nowMonth")
      .withColumn("open", col("open").cast(DecimalType(10, 2)))
      .withColumn("close", col("close").cast(DecimalType(10, 2)))
      .withColumn("volume", col("volume").cast(IntegerType))
      .withColumn("high", col("high").cast(DecimalType(10, 2)))
      .withColumn("low", col("low").cast(DecimalType(10, 2)))
      .withColumn("amount", col("amount").cast(IntegerType))
      .withColumn("change", col("change").cast(DecimalType(10, 2)))
      .withColumn("change_pct", col("change_pct").cast(DecimalType(10, 2)))
      .withColumn("pre_close", col("pre_close").cast(DecimalType(10, 2)))

    df2.cache()
    df2.createTempView("ta2")

    val df3 = spark.sql("select *,row_number() over(partition by stock_code order by trade_date,stock_code) as row from ta2 ")
    df3.cache()
    df3.createTempView("ta3")
    //TODO 连板数
    val df4 = spark.sql(
      """
        |select t2.*,
        |if(t2.open<t1.close,'低开',if(t2.open=t1.close,'平开','高开')) as kp,
        |if(t2.open<t2.close,'高走',if(t2.open=t1.close,'平走','低走')) as wp,
        |if(t2.high<t1.high,'下探',if(t2.high=t1.high,'平整','突破')) as sx,
        |if(t2.low<t1.low,'下探',if(t2.low=t1.low,'平整','突破')) as xx,
        |if(t2.volume<t1.volume,'缩量',if(t2.volume=t1.volume,'平量',if(t2.volume/t1.volume>2,'放巨量','放量'))) as ln,
        |round(t2.volume/t1.volume,2) as zrlnb,
        |case when t2.open<t2.close and (t2.close-t2.open>=t2.high-t2.close+t2.open-t2.low or (t2.close-t2.open>=t2.high-t2.close and t2.close-t2.open>=t2.open-t2.low)) then '红柱'
        |	 when t2.open<t2.close and (t2.high-t2.close>=t2.close-t2.low or (t2.high-t2.close>=t2.close-t2.open and t2.high-t2.close>=t2.open-t2.low and t2.close-t2.open>=t2.open-t2.low)) then '红柱上影线'
        |	 when t2.open<t2.close and (t2.open-t2.low>=t2.high-t2.open or (t2.open-t2.low>=t2.close-t2.open and t2.open-t2.low>=t2.high-t2.close and t2.close-t2.open>=t2.high-t2.close)) then '红柱下影线'
        |	 when (t2.open<t2.close or (t2.open=t2.close and t2.open>t2.pre_close)) and t2.high-t2.close>=t2.close-t2.open and t2.open-t2.low>=t2.close-t2.open and t2.high!=t2.low then '红柱十字星'
        |	 when t2.close<t2.open and (t2.open-t2.close>=t2.high-t2.open+t2.close-t2.low or (t2.open-t2.close>=t2.high-t2.open and t2.open-t2.close>=t2.close-t2.low)) then '绿柱'
        |	 when t2.close<t2.open and (t2.high-t2.open>=t2.open-t2.low or (t2.high-t2.open>=t2.close-t2.low and t2.high-t2.open>=t2.open-t2.close and t2.open-t2.close>=t2.close-t2.low)) then '绿柱上影线'
        |	 when t2.close<t2.open and (t2.close-t2.low>=t2.high-t2.close or (t2.close-t2.low>=t2.high-t2.open and t2.close-t2.low>=t2.open-t2.close and t2.open-t2.close>=t2.high-t2.open))  then '绿柱下影线'
        |	 when (t2.open>t2.close or (t2.open=t2.close and t2.open<t2.pre_close)) and t2.high-t2.open>=t2.open-t2.close and t2.close-t2.low>=t2.open-t2.close and t2.high!=t2.low  then '绿柱十字星'
        |	 when t2.high=t2.open and t2.open=t2.close and t2.close=t2.low and t2.change_pct>9 then '涨停一字'
        |	 when t2.high=t2.open and t2.open=t2.close and t2.close=t2.low and t2.change_pct<-9 then '跌停一字'
        |	else '其他'
        |end as zt
        |from ta3 as t1 left join ta3 as t2 on t1.row+1=t2.row and t1.stock_code=t2.stock_code
        |where t2.row is not null and t2.stock_code like '00%' or t2.stock_code like '60%'""".stripMargin)
    df4.cache()
    df4.createTempView("ta4")


    spark.sql(
      """|select t0.stock_code,substring(t0.trade_date,0,7) as trade_date_month,
         |t0.trade_date as t0_trade_date,t0.sfzt as t0_sfzt, t0.open as t0_open , t0.close as t0_close ,t0.high as t0_high,t0.low as t0_low,t0.volume as t0_volume,t0.amount as t0_amount,t0.pre_close as t0_pre_close,t0.kp as t0_kp,t0.wp as t0_wp ,t0.sx as t0_sx,t0.xx as t0_xx,t0.ln as t0_ln,t0.zrlnb as t0_zrlnb,t0.zt as t0_zt,t0.turnover_ratio as t0_turnover_ratio,t0.change as t0_change,t0.change_pct as t0_change_pct,
         |t1.trade_date as t1_trade_date,t1.sfzt as t1_sfzt, t1.open as t1_open , t1.close as t1_close ,t1.high as t1_high,t1.low as t1_low,t1.volume as t1_volume,t1.amount as t1_amount,t1.pre_close as t1_pre_close,t1.kp as t1_kp,t1.wp as t1_wp ,t1.sx as t1_sx,t1.xx as t1_xx,t1.ln as t1_ln,t1.zrlnb as t1_zrlnb,t1.zt as t1_zt,t1.turnover_ratio as t1_turnover_ratio,t1.change as t1_change,t1.change_pct as t1_change_pct,
         |t2.trade_date as t2_trade_date,t2.sfzt as t2_sfzt, t2.open as t2_open , t2.close as t2_close ,t2.high as t2_high,t2.low as t2_low,t2.volume as t2_volume,t2.amount as t2_amount,t2.pre_close as t2_pre_close,t2.kp as t2_kp,t2.wp as t2_wp ,t2.sx as t2_sx,t2.xx as t2_xx,t2.ln as t2_ln,t2.zrlnb as t2_zrlnb,t2.zt as t2_zt,t2.turnover_ratio as t2_turnover_ratio,t2.change as t2_change,t2.change_pct as t2_change_pct,
         |t3.trade_date as t3_trade_date,t3.sfzt as t3_sfzt, t3.open as t3_open , t3.close as t3_close ,t3.high as t3_high,t3.low as t3_low,t3.volume as t3_volume,t3.amount as t3_amount,t3.pre_close as t3_pre_close,t3.kp as t3_kp,t3.wp as t3_wp ,t3.sx as t3_sx,t3.xx as t3_xx,t3.ln as t3_ln,t3.zrlnb as t3_zrlnb,t3.zt as t3_zt,t3.turnover_ratio as t3_turnover_ratio,t3.change as t3_change,t3.change_pct as t3_change_pct,
         |t4.trade_date as t4_trade_date,t4.sfzt as t4_sfzt, t4.open as t4_open , t4.close as t4_close ,t4.high as t4_high,t4.low as t4_low,t4.volume as t4_volume,t4.amount as t4_amount,t4.pre_close as t4_pre_close,t4.kp as t4_kp,t4.wp as t4_wp ,t4.sx as t4_sx,t4.xx as t4_xx,t4.ln as t4_ln,t4.zrlnb as t4_zrlnb,t4.zt as t4_zt,t4.turnover_ratio as t4_turnover_ratio,t4.change as t4_change,t4.change_pct as t4_change_pct,
         |t5.trade_date as t5_trade_date,t5.sfzt as t5_sfzt, t5.open as t5_open , t5.close as t5_close ,t5.high as t5_high,t5.low as t5_low,t5.volume as t5_volume,t5.amount as t5_amount,t5.pre_close as t5_pre_close,t5.kp as t5_kp,t5.wp as t5_wp ,t5.sx as t5_sx,t5.xx as t5_xx,t5.ln as t5_ln,t5.zrlnb as t5_zrlnb,t5.zt as t5_zt,t5.turnover_ratio as t5_turnover_ratio,t5.change as t5_change,t5.change_pct as t5_change_pct,
         |t6.trade_date as t6_trade_date,t6.sfzt as t6_sfzt, t6.open as t6_open , t6.close as t6_close ,t6.high as t6_high,t6.low as t6_low,t6.volume as t6_volume,t6.amount as t6_amount,t6.pre_close as t6_pre_close,t6.kp as t6_kp,t6.wp as t6_wp ,t6.sx as t6_sx,t6.xx as t6_xx,t6.ln as t6_ln,t6.zrlnb as t6_zrlnb,t6.zt as t6_zt,t6.turnover_ratio as t6_turnover_ratio,t6.change as t6_change,t6.change_pct as t6_change_pct,
         |t7.trade_date as t7_trade_date,t7.sfzt as t7_sfzt, t7.open as t7_open , t7.close as t7_close ,t7.high as t7_high,t7.low as t7_low,t7.volume as t7_volume,t7.amount as t7_amount,t7.pre_close as t7_pre_close,t7.kp as t7_kp,t7.wp as t7_wp ,t7.sx as t7_sx,t7.xx as t7_xx,t7.ln as t7_ln,t7.zrlnb as t7_zrlnb,t7.zt as t7_zt,t7.turnover_ratio as t7_turnover_ratio,t7.change as t7_change,t7.change_pct as t7_change_pct,
         |t8.trade_date as t8_trade_date,t8.sfzt as t8_sfzt, t8.open as t8_open , t8.close as t8_close ,t8.high as t8_high,t8.low as t8_low,t8.volume as t8_volume,t8.amount as t8_amount,t8.pre_close as t8_pre_close,t8.kp as t8_kp,t8.wp as t8_wp ,t8.sx as t8_sx,t8.xx as t8_xx,t8.ln as t8_ln,t8.zrlnb as t8_zrlnb,t8.zt as t8_zt,t8.turnover_ratio as t8_turnover_ratio,t8.change as t8_change,t8.change_pct as t8_change_pct

         | from ta4 as t0
         | left join ta4 as t1 on t0.row+1=t1.row and t0.stock_code=t1.stock_code
         | left join ta4 as t2 on t0.row+2=t2.row and t0.stock_code=t2.stock_code
         | left join ta4 as t3 on t0.row+3=t3.row and t0.stock_code=t3.stock_code
         | left join ta4 as t4 on t0.row+4=t4.row and t0.stock_code=t4.stock_code
         | left join ta4 as t5 on t0.row+5=t5.row and t0.stock_code=t5.stock_code
         | left join ta4 as t6 on t0.row+6=t6.row and t0.stock_code=t6.stock_code
         | left join ta4 as t7 on t0.row+7=t7.row and t0.stock_code=t7.stock_code
         | left join ta4 as t8 on t0.row+8=t8.row and t0.stock_code=t8.stock_code

         |""".stripMargin).createTempView("ta5")

    spark.sql("select * from ta5")
      .repartition(8).write.mode("overwrite").partitionBy("trade_date_month").parquet("file:///D:\\gsdata\\gpsj_9days_month")

    spark.sql(
      """|select t0.stock_code,substring(t0.trade_date,0,7) as trade_date_month,
         |t0.trade_date as t0_trade_date,
         | t9.trade_date as  t9_trade_date, t9.sfzt as  t9_sfzt,  t9.open as  t9_open ,  t9.close as  t9_close , t9.high as  t9_high, t9.low as  t9_low, t9.volume as  t9_volume, t9.amount as  t9_amount, t9.pre_close as   t9_pre_close, t9.kp as  t9_kp, t9.wp as  t9_wp , t9.sx as  t9_sx, t9.xx as  t9_xx, t9.ln as  t9_ln, t9.zrlnb as  t9_zrlnb, t9.zt as  t9_zt, t9.turnover_ratio as  t9_turnover_ratio, t9.change as  t9_change, t9.change_pct as  t9_change_pct,
         |t10.trade_date as t10_trade_date,t10.sfzt as t10_sfzt, t10.open as t10_open , t10.close as t10_close ,t10.high as t10_high,t10.low as t10_low,t10.volume as t10_volume,t10.amount as t10_amount,t10.pre_close as  t10_pre_close,t10.kp as t10_kp,t10.wp as t10_wp ,t10.sx as t10_sx,t10.xx as t10_xx,t10.ln as t10_ln,t10.zrlnb as t10_zrlnb,t10.zt as t10_zt,t10.turnover_ratio as t10_turnover_ratio,t10.change as t10_change,t10.change_pct as t10_change_pct,
         |t11.trade_date as t11_trade_date,t11.sfzt as t11_sfzt, t11.open as t11_open , t11.close as t11_close ,t11.high as t11_high,t11.low as t11_low,t11.volume as t11_volume,t11.amount as t11_amount,t11.pre_close as  t11_pre_close,t11.kp as t11_kp,t11.wp as t11_wp ,t11.sx as t11_sx,t11.xx as t11_xx,t11.ln as t11_ln,t11.zrlnb as t11_zrlnb,t11.zt as t11_zt,t11.turnover_ratio as t11_turnover_ratio,t11.change as t11_change,t11.change_pct as t11_change_pct,
         |t12.trade_date as t12_trade_date,t12.sfzt as t12_sfzt, t12.open as t12_open , t12.close as t12_close ,t12.high as t12_high,t12.low as t12_low,t12.volume as t12_volume,t12.amount as t12_amount,t12.pre_close as  t12_pre_close,t12.kp as t12_kp,t12.wp as t12_wp ,t12.sx as t12_sx,t12.xx as t12_xx,t12.ln as t12_ln,t12.zrlnb as t12_zrlnb,t12.zt as t12_zt,t12.turnover_ratio as t12_turnover_ratio,t12.change as t12_change,t12.change_pct as t12_change_pct,
         |t13.trade_date as t13_trade_date,t13.sfzt as t13_sfzt, t13.open as t13_open , t13.close as t13_close ,t13.high as t13_high,t13.low as t13_low,t13.volume as t13_volume,t13.amount as t13_amount,t13.pre_close as  t13_pre_close,t13.kp as t13_kp,t13.wp as t13_wp ,t13.sx as t13_sx,t13.xx as t13_xx,t13.ln as t13_ln,t13.zrlnb as t13_zrlnb,t13.zt as t13_zt,t13.turnover_ratio as t13_turnover_ratio,t13.change as t13_change,t13.change_pct as t13_change_pct,
         |t14.trade_date as t14_trade_date,t14.sfzt as t14_sfzt, t14.open as t14_open , t14.close as t14_close ,t14.high as t14_high,t14.low as t14_low,t14.volume as t14_volume,t14.amount as t14_amount,t14.pre_close as  t14_pre_close,t14.kp as t14_kp,t14.wp as t14_wp ,t14.sx as t14_sx,t14.xx as t14_xx,t14.ln as t14_ln,t14.zrlnb as t14_zrlnb,t14.zt as t14_zt,t14.turnover_ratio as t14_turnover_ratio,t14.change as t14_change,t14.change_pct as t14_change_pct
         | from ta4 as t0
         | left join ta4 as  t9 on t0.row+9 = t9.row and t0.stock_code= t9.stock_code
         | left join ta4 as t10 on t0.row+10=t10.row and t0.stock_code=t10.stock_code
         | left join ta4 as t11 on t0.row+11=t11.row and t0.stock_code=t11.stock_code
         | left join ta4 as t12 on t0.row+12=t12.row and t0.stock_code=t12.stock_code
         | left join ta4 as t13 on t0.row+13=t13.row and t0.stock_code=t13.stock_code
         | left join ta4 as t14 on t0.row+14=t14.row and t0.stock_code=t14.stock_code

         |""".stripMargin).createTempView("ta6")

    spark.sql("select * from ta6")
      .repartition(8).write.mode("overwrite").partitionBy("trade_date_month").parquet("file:///D:\\gsdata\\gpsj_h6days_month")

    df2.unpersist()
    df3.unpersist()
    df4.unpersist()
  }
}
