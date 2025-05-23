package sparktask.mysql.update

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Update15days {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.sql.shuffle.partitions","3000")
      .set("spark.driver.memory","8g")
      .set("spark.local.dir","D:\\SparkTemp")
    val spark = SparkSession
      .builder()
      .appName("Update15days")
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println(spark.read.parquet("file:///D:/gsdata/gpsj_9days/trade_date_month=2023-03").count())

    val start = System.currentTimeMillis()
    all15days(spark)
    val end = System.currentTimeMillis()
    print("共耗时："+(end-start)/1000+"秒")

//    val start = System.currentTimeMillis()
//    all15daysForMonth(spark,"2024-07")
//    val end = System.currentTimeMillis()
//    print("共耗时："+(end-start)/1000+"秒")


    spark.close()
  }

  /**
   * 以t0的时间进行分区
   */
  //substring(ta1.t0_trade_date,0,7) as trade_date_month
  def all15days(spark:SparkSession)={
    val df1 = spark.read.parquet("file:///D:\\gsdata\\gpsj_9days").repartition(1000)
    df1.createTempView("ta1")

    val df2 = spark.read.parquet("file:///D:\\gsdata\\gpsj_h6days").repartition(1000)
    df2.createTempView("ta2")

    val duplicateColumns = df1.columns.intersect(df2.columns)
    val ta2str = df2.columns.diff(duplicateColumns).map(f=>"ta2."+f).mkString(",")

    spark.sql(s"select ta1.*,$ta2str  from ta1 left join ta2 on ta1.stock_code=ta2.stock_code and ta1.t0_trade_date=ta2.t0_trade_date").repartition(6)
      .write.mode("overwrite").partitionBy("trade_date_month").parquet("file:///D:\\gsdata\\gpsj_15days")

  }


  def all15daysForMonth(spark:SparkSession,month:String)={
    val df1 = spark.read.parquet(s"file:///D:\\gsdata\\gpsj_9days_month\\trade_date_month=$month")
    df1.createTempView("ta1")

    val df2 = spark.read.parquet(s"file:///D:\\gsdata\\gpsj_h6days_month\\trade_date_month=$month")
    df2.createTempView("ta2")

    val duplicateColumns = df1.columns.intersect(df2.columns)
    val ta2str = df2.columns.diff(duplicateColumns).map(f=>"ta2."+f).mkString(",")

        spark.sql(
          s"""select ta1.*,$ta2str ,substring(ta1.t0_trade_date,0,7) as trade_date_month
             | from ta1 left join ta2 on ta1.stock_code=ta2.stock_code and ta1.t0_trade_date=ta2.t0_trade_date
             |""".stripMargin)
          .repartition(4)
          .write.mode("overwrite")
          .partitionBy("trade_date_month")
          .parquet("file:///D:\\gsdata\\gpsj_15days_month")
  }
}
