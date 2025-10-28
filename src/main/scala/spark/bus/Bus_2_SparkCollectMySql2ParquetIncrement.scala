package spark.bus

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
 * 总线2
 * 将mysql中日数据采集生成parquet文件【增量采集】
 * ods原始数据层
 */
object Bus_2_SparkCollectMySql2ParquetIncrement {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions","10")
      .set("spark.driver.memory","4g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "10")
      .set("spark.local.dir","D:\\SparkTemp")

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val url = "jdbc:mysql://localhost:3306/gs"
    val properties = getMysqlProperties()
    val startm = System.currentTimeMillis()

//    val jyrlsAll = ArrayBuffer("data2024_gpsj_day_1219")
    val jyrlsSome = ArrayBuffer(
      "data_gpsj_day_20251027"
    )

    /**
     * 增量股票数据更新
     */
    val CompleteSinkPath = "gpsj_day_all_hs"
    updateGpsjIncrement(updateGpsj(spark,url,jyrlsSome,properties),CompleteSinkPath)

    val endm = System.currentTimeMillis()
    println("共耗时："+(endm-startm)/1000+"秒")
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
    properties.setProperty("partitionColumn", "amount") // 选择一个分区列
    properties.setProperty("lowerBound", "10000") // 分区列的最小值
    properties.setProperty("upperBound", "1000000") // 分区列的最大值
    properties.setProperty("numPartitions", "8") // 设置分区数量
    properties
  }

  /**
   * 只做增量
   * @param df
   * @param CompleteSinkPath
   */
  def updateGpsjIncrement(df:DataFrame,CompleteSinkPath:String)={
    df.distinct().repartition(1).write.mode("append")
      .partitionBy("trade_date_month")
      .parquet(s"file:///D:\\gsdata\\$CompleteSinkPath")

  }

  /**
   * 按天更新股票数据
   *
   */
  def updateGpsj(spark: SparkSession,url:String,jyrls:ArrayBuffer[String],properties: Properties): DataFrame = {
    var df:DataFrame = spark.read.jdbc(url, jyrls(0), properties)
    if(jyrls.size>1){
      for(i<-1 until jyrls.size){
        println("准备处理："+jyrls(i))
        df = df.union(spark.read.jdbc(url, jyrls(i), properties))
        println("处理完毕："+jyrls(i))
      }
    }

    df.withColumn("open", col("open").cast(DecimalType(10, 2)))
      .withColumn("close", col("close").cast(DecimalType(10, 2)))
      .withColumn("volume", col("volume").cast(IntegerType))
      .withColumn("high", col("high").cast(DecimalType(10, 2)))
      .withColumn("low", col("low").cast(DecimalType(10, 2)))
      .withColumn("amount", col("amount").cast(IntegerType))
      .withColumn("change", col("change").cast(DecimalType(10, 2)))
      .withColumn("change_pct", col("change_pct").cast(DecimalType(10, 2)))
      .withColumn("turnover_ratio", col("turnover_ratio").cast(StringType))
      .withColumn("pre_close", col("pre_close").cast(DecimalType(10, 2)))
      .createOrReplaceTempView("ta1")
    //按月保存每日数据

    val df_hs = spark.sql(
      """select trade_time,open,close,volume,high,low,amount,change,change_pct,turnover_ratio,pre_close,stock_code,trade_date,
        |if(close>=round(pre_close*1.1,2),1,0) as sfzt,
        |if(high>=round(pre_close*1.1,2) and close<round(pre_close*1.1,2),1,0) as cjzt,
        |if(close<=round(pre_close*0.9,2),1,0) as sfdt,
        |if(low<=round(pre_close*0.9,2) and close>round(pre_close*0.9,2),1,0) as cjdt,
        |if(open<pre_close,'低开',if(open=pre_close,'平开','高开')) as kp,
        |if(open<close,'高走',if(open=close,'平走','低走')) as wp,
        |round((open-pre_close)/pre_close*100,2) as kpzf,
        |round((close-pre_close)/pre_close*100,2) as spzf,
        |round((high-pre_close)/pre_close*100,2) as zgzf,
        |round((low-pre_close)/pre_close*100,2) as zdzf,
        |round((high-pre_close)/pre_close*100-(low-pre_close)/pre_close*100,2) as qjzf,
        |round((close-pre_close)/pre_close*100-(open-pre_close)/pre_close*100,2) as stzf,
        |case when open<close and (close-open>=high-close+open-low or (close-open>=high-close and close-open>=open-low)) then '红柱'
        |	 when open<close and (high-close>=close-low or (high-close>=close-open and high-close>=open-low and close-open>=open-low)) then '红柱上影线'
        |	 when open<close and (open-low>=high-open or (open-low>=close-open and open-low>=high-close and close-open>=high-close)) then '红柱下影线'
        |	 when (open<close or (open=close and open>pre_close)) and high-close>=close-open and open-low>=close-open and high!=low then '红柱十字星'
        |	 when close<open and (open-close>=high-open+close-low or (open-close>=high-open and open-close>=close-low)) then '绿柱'
        |	 when close<open and (high-open>=open-low or (high-open>=close-low and high-open>=open-close and open-close>=close-low)) then '绿柱上影线'
        |	 when close<open and (close-low>=high-close or (close-low>=high-open and close-low>=open-close and open-close>=high-open))  then '绿柱下影线'
        |	 when (open>close or (open=close and open<pre_close)) and high-open>=open-close and close-low>=open-close and high!=low  then '绿柱十字星'
        |	 when high=open and open=close and close=low and change_pct>9 then '涨停一字'
        |	 when high=open and open=close and close=low and change_pct<-9 then '跌停一字'
        |	else '其他'
        |end as kxzt,
        |substring(trade_date,0,7) as trade_date_month,
        |if(stock_code like '60%','sh',if(stock_code like '00%','sz',if(stock_code like '30%','cy',if(stock_code like '68%','kc','')))) as bk
        |from ta1  order by stock_code""".stripMargin)
      .drop("index").filter("bk in ('sh','sz')")


//    df_hs.where("stock_code in ('605179','002769','002530','002775','002593','603238') and trade_time='2024-12-13 00:00:00'").show()

      //获取agdm并标记ST股票
//      var agdmdf:DataFrame = spark.read.jdbc(url, agdm, properties)

      df_hs
  }

}
