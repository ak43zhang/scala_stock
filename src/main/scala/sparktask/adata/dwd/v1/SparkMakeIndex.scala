package sparktask.adata.dwd.v1

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 指标生成
 */
object SparkMakeIndex {
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

    val startm = System.currentTimeMillis()

    val df2 = spark.read.parquet("file:///D:\\gsdata\\gpsj_day_all_hs\\trade_date_month=2024-12")
    df2.cache()
    df2.createTempView("ta1")
    df2.printSchema()

    //成交量
    spark.sql(
      """select trade_date,
        |sum(amount) as `沪深成交量`,
        |sum(if(bk='sz',amount,0))  as `深圳成交量`,
        |sum(if(bk='sh',amount,0))  as `上海成交量`
        |from ta1 group by trade_date order by trade_date desc""".stripMargin).show()

    //板块成交量

    val url = "jdbc:mysql://localhost:3306/gs"
    val driver = "com.mysql.cj.jdbc.Driver"
    val user = "root"
    val pwd = "123456"

    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", pwd)
    properties.setProperty("url", url)
    properties.setProperty("driver", driver)

    val df3:DataFrame = spark.read.jdbc(url, "data2024_gnzscfxx_ths", properties)
    df3.printSchema()
    df3.createTempView("ta2")

    val df4:DataFrame = spark.read.jdbc(url, "data2024_gnzsxx_ths", properties)
    df4.printSchema()
    df4.createTempView("ta3")

//    spark.sql("select * from ta2").show(100)
//
//    spark.sql("select * from ta3").show(100)

    spark.sql("select ta2.*,ta3.name from ta2 left join ta3 on ta2.index_code=ta3.index_code").createTempView("ta4")

    spark.sql("""select * from ta1 left join ta4 on ta1.stock_code = ta4.stock_code""".stripMargin).drop("index").createTempView("ta5")

    spark.sql("select trade_date,index_code,name,sum(amount) as bkcjl from ta5 group by trade_date,index_code,name order by trade_date desc,name").createTempView("ta6")

//    spark.sql("select *,row_number() over(partition by index_code,name order by trade_date,index_code,name) as row from ta6 ").createTempView("ta7")
//
//    spark.sql("select t1.*,t2.bkcjl as t2_bkcjl,t2.bkcjl/t1.bkcjl as bkcjlbfb from ta7 as t1 left join ta7 as t2 on t1.row+1=t2.row and t1.index_code=t2.index_code order by t1.trade_date desc,t2.bkcjl/t1.bkcjl desc")
//      .filter("trade_date='2024-12-26'").show(500)

    //TODO 优化
    spark.sql(
      """select
        |trade_date,index_code,name,bkcjl as `今日成交量`,
        |sum(bkcjl) over(partition by index_code,name ORDER BY index_code,name,row ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) as `昨日成交量`,
        |sum(bkcjl) over(partition by index_code,name ORDER BY index_code,name,row ROWS BETWEEN 2 PRECEDING AND 2 PRECEDING) as `前日成交量`
        |from
        |(select *,row_number() over(partition by index_code,name order by trade_date,index_code,name) as row from ta6) """.stripMargin).where("trade_date='2024-12-25'").show()


    val endm = System.currentTimeMillis()
    println("共耗时："+(endm-startm)/1000+"秒")
    spark.close()
  }
}
