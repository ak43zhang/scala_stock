package sparktask.adata.dws

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import sparktask.adata.collection.SparkCollectMySql2ParquetIncrement.getMysqlProperties

object Spark2Dws {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "10")
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

    val startm = System.currentTimeMillis()

    val df1 = spark.read.parquet("file:///D:\\gsdata\\gpsj_day_all_hs\\trade_date_month=202*")
    df1.printSchema()
    df1.createTempView("ta1")

    qz_bank_or_other_ths(spark)

//    qz_or_other_ths(spark)

//    spark.sql(
//      """
//        |select trade_date,name,sum(spzf)/count(1) as `板块涨幅` from (select ta1.*,ta3.short_name,ta3.index_code,ta3.name  from ta1 left join  ta3 on ta1.stock_code=ta3.stock_code)
//        |group by trade_date,name order by trade_date
//        |""".stripMargin).where("name='银行'").show()




    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()

  }

  /**
   * 大盘分析
   */
  def dp_fx(spark:SparkSession): Unit ={
    /**
     * 大盘状态指标
     * 红盘数  涨停数 翘板数 下影线数（低转高）
     * 绿盘数  跌停数 破板数 上隐线数（高转低）
     *
     */
    spark.sql(
      """select trade_date,
        |sum(if(kxzt='涨停一字',1,0)) as `涨停一字`,
        |sum(if(kxzt='红柱',1,0)) as `红柱`,
        |sum(if(kxzt='红柱下影线',1,0)) as `红柱下影线`,
        |sum(if(kxzt='红柱十字星',1,0)) as `红柱十字星`,
        |sum(if(kxzt='绿柱下影线',1,0)) as `绿柱下影线`,
        |sum(if(kxzt='绿柱十字星',1,0)) as `绿柱十字星`,
        |sum(if(kxzt='红柱上影线',1,0)) as `红柱上影线`,
        |sum(if(kxzt='绿柱上影线',1,0)) as `绿柱上影线`,
        |sum(if(kxzt='绿柱',1,0)) as `绿柱`,
        |sum(if(kxzt='跌停一字',1,0)) as `跌停一字`,
        |sum(if(kxzt='其他',1,0)) as `其他`,
        |round(sum(if(kxzt in ('红柱','红柱上影线','红柱下影线','红柱十字星','涨停一字'),1,0))/count(1)*100,2) as `红股比率`,
        |round(sum(if(kxzt in ('绿柱','绿柱上影线','绿柱下影线','绿柱十字星','跌停一字'),1,0))/count(1)*100,2) as `绿股比率`,
        |round(sum(if(kxzt in ('红柱','红柱下影线','绿柱下影线'),1,0))/sum(if(kxzt in ('绿柱','红柱上影线','绿柱上影线'),1,0))*100,2) as `投机比`,
        |sum(if(sfzt='1',1,0)) as `涨停数`,
        |sum(if(cjzt='1',1,0)) as `炸板数`,
        |sum(if(sfdt='1',1,0)) as `跌停数`,
        |sum(if(cjdt='1',1,0)) as `翘板数`,
        |round(sum(if(sfzt='1',1,0))/(sum(if(sfzt='1',1,0))+sum(if(cjzt='1',1,0)))*100,2) as `封板率`,
        |round(sum(if(cjzt='1',1,0))/(sum(if(sfzt='1',1,0))+sum(if(cjzt='1',1,0)))*100,2) as `炸板率`
        | from ta1 group by trade_date order by trade_date desc""".stripMargin).show(200)
  }

  /**
   * 个股分析
   */
  def ggfx(spark:SparkSession): Unit ={
    /**
     * 个股相关指标
     */
    spark.sql(
      """select stock_code,
        |sum(if(sfzt=1,1,0)) as `涨停次数`,
        |sum(if(cjzt=1,1,0)) as `炸板次数`,
        |sum(if(kxzt in ('绿柱上影线','红柱上影线') and zgzf>8,1,0)) as `上影线数量`
        |from ta1 where trade_date like '2024%'
        |group by stock_code order by sum(if(sfzt=1,1,0)) desc""".stripMargin).orderBy(col("上影线数量").desc).show(500)


    //涨跌点
    //    val df = spark.sql("select trade_date,cast(sum(spzf)/count(1) as double) as sp from ta1 where trade_date>'2024-01-01' and trade_date<'2025-01-01' group by trade_date order by trade_date ")
    //    df.printSchema()
    //    df.show(400)
    //    var zs = 1.0
    //    df.collect().sortBy(f=>f.getAs[String]("trade_date")).foreach(f=>{
    //      val date = f.getAs[String]("trade_date")
    //      zs = zs*(1+f.getAs[Double]("sp")/100)
    //      println(date+": "+zs)
    //    })
    //    println("最终："+zs)
  }

  /**
   * 权重和其他板块的开盘和收盘比较，结合大盘做分析
   */
  def qz_or_other_ths(spark:SparkSession): Unit ={
    val url = "jdbc:mysql://localhost:3306/gs"
    val driver = "com.mysql.cj.jdbc.Driver"
    val user = "root"
    val pwd = "123456"

    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", pwd)
    properties.setProperty("url", url)
    properties.setProperty("driver", driver)

    val gnxx = "data2024_gnzsxx_ths"
    val gncfxx = "data2024_gnzscfxx_ths"
    var df:DataFrame = spark.read.jdbc(url, gnxx, properties)
    df.createTempView("t1")

    var df2:DataFrame = spark.read.jdbc(url, gncfxx, properties)
    df2.createTempView("t2")

    val df3 = spark.sql(
      """
        |select t2.*,t1.name from t2 left join t1 on t2.index_code = t1.index_code
        |""".stripMargin)
    df3.createTempView("ta3")



  }

  /**
   * 利好指标
   */
  def lhzb(): Unit ={

  }

  /**
   * 利空指标
   */
  def lkzb(): Unit ={

  }


  /**
   * 银行权重和其他板块的开盘和收盘比较，结合大盘做分析
   */
  def qz_bank_or_other_ths(spark:SparkSession): Unit ={
    val url = "jdbc:mysql://localhost:3306/gs"
    val driver = "com.mysql.cj.jdbc.Driver"
    val user = "root"
    val pwd = "123456"

    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", pwd)
    properties.setProperty("url", url)
    properties.setProperty("driver", driver)

    val gnxx = "data2024_dzgpssgn_east"
    var df:DataFrame = spark.read.jdbc(url, gnxx, properties)
    df.createTempView("ta2")

    //,sum(kpzf) over(partition by plate_name,trade_date order by plate_name,trade_date) as ss1
    val df3 = spark.sql(
      """
        |select sum(kpzf) as s1,sum(spzf) as s2,count(1) as sl,plate_name,trade_date from ta1 left join (select * from ta2 where plate_type='行业') as ta2 on ta1.stock_code =ta2.stock_code group by plate_name,trade_date
        |""".stripMargin)

    df3.where("plate_name='银行'").createTempView("ta3")
//      .orderBy(col("s").desc).show(300)

    spark.sql("select * from ta3 where s1>0 ").show()

  }
}
