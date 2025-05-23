package sparktask.mysql

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object SqlIn {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled","true")
    val spark = SparkSession
      .builder()
      .appName("SqlIn")
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val url = "jdbc:mysql://localhost:3306/gs"

    val driver = "com.mysql.cj.jdbc.Driver"
    val user = "root"
    val pwd = "123456"

    val properties = new Properties()
    properties.setProperty("user",user)
    properties.setProperty("password",pwd)
    properties.setProperty("url",url)
    properties.setProperty("driver",driver)
    val jyrl = "data2024_jyrl"
    import spark.implicits._
    spark.read.jdbc(url, jyrl, properties).createTempView("ta1")
    spark.sql(
      """select trade_date as td,row_number() over(partition by '1' order by '1') as row from ta1 where trade_status=1 """.stripMargin).createTempView("ta2")

    //有涨停，主板，非st，2024年2月7日缩量下跌，2024年2月8日缩量下跌，2024年2月9日日缩量下跌，2024年2月6日-2024年2月8日跌幅大于百分之6
    val resdf = spark.sql(
      """select ta2.td as d1,tt2.td as d2,tt3.td as d3,tt4.td as d4 from ta2 left join ta2 as tt2 on ta2.row+1=tt2.row
         left join ta2 as tt3 on tt2.row+1=tt3.row
         left join ta2 as tt4 on tt3.row+1=tt4.row
        order by tt2.row""".stripMargin)
      .flatMap(f=>{
        val arr = new ArrayBuffer[String]
        val d1s = f.getAs[String]("d1")
        val d2s = f.getAs[String]("d2")
        val d3s = f.getAs[String]("d3")
        val d4s = f.getAs[String]("d4")
        val originalFormat = new SimpleDateFormat("yyyy-MM-dd")
        val targetFormat = new SimpleDateFormat("yyyy年MM月dd日")

        if(d2s!=null && d3s!=null && d4s!=null){
          val date1 = originalFormat.parse(d1s)
          val d1 = targetFormat.format(date1)
          val date2 = originalFormat.parse(d2s)
          val d2 = targetFormat.format(date2)
          val date3 = originalFormat.parse(d3s)
          val d3 = targetFormat.format(date3)
          val date4 = originalFormat.parse(d4s)
          val d4 = targetFormat.format(date4)
          //2024年02月28日涨幅大于百分之5，2024年02月28日最高价大于2024年02月29日最高价，主板，非st，2024年02月29日缩量下跌，2024年03月01日缩量下跌，2024年03月04日缩量下跌
          arr+= (s"${d1}涨幅大于百分之5，${d1}最高价大于${d2}最高价，主板，非st，${d2}缩量下跌，${d3}缩量下跌，${d4}缩量下跌，${d2}-${d4}跌幅大于百分之4")

        }
      arr


    })
    resdf.show(false)
    resdf.repartition(1).write.text("file:///C:\\Users\\Administrator\\Desktop\\res\\yj2")
    spark.close()
  }
}
