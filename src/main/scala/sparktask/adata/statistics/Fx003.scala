package sparktask.adata.statistics

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import sparktask.tools.MysqlTools

import scala.collection.mutable.ArrayBuffer

/**
 * 分析排除风险数据后的盈亏情况
 */
object Fx003 {
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

    val url = "jdbc:mysql://localhost:3306/gs"
    val driver = "com.mysql.cj.jdbc.Driver"
    val user = "root"
    val pwd = "123456"

    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", pwd)
    properties.setProperty("url", url)
    properties.setProperty("driver", driver)

    var df:DataFrame = spark.read.jdbc(url, "data_jyrl", properties)
    df.createTempView("data_jyrl")
    val listsetDate = spark.sql("select trade_date from data_jyrl where  trade_date between '2024-07-01' and '2024-12-04' and trade_status='1' order by trade_date desc")
      .collect()
      .map(f=>f.getAs[String]("trade_date").replaceAll("-",""))


    for(setdate<-listsetDate){
      val formattedDate = s"${setdate.take(4)}-${setdate.slice(4, 6)}-${setdate.takeRight(2)}"
      var df:DataFrame = spark.read.jdbc(url, s"wencaiquery_basequery_$setdate", properties).select("`代码`","`简称`")
      df.createTempView("ta1")

      var df2:DataFrame = spark.read.jdbc(url, s"wencaiquery_venture_$setdate", properties)
      df2.createTempView("ta2")

      val df3 = spark.sql(s"select ta1.*,ta2.fxlx,'$formattedDate' as trade_date from ta1 left join (select `代码`, `简称`,concat_ws(',',collect_set(`风险类型`)) as fxlx from ta2 group by `代码`, `简称`) as ta2 on ta1.`代码`=ta2.`代码` ")
          .where("fxlx is null")
      df3.createTempView("ta3")

      val df4 = spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_10days\\trade_date_month=202[3,4,5]*")
        .where("t1_zgzf>=8")
      df4.createTempView("ta4")

      val resdf: DataFrame = spark.sql(
        """select stock_code,`简称` as jc,t1_trade_date,t1_sfzt,t1_cjzt,
          |t0_zgzf-t0_spzf as dddf,t1_kpzf,t1_zgzf,t1_spzf,t2_kpzf,t2_zgzf,t2_zdzf,t2_spzf,
          |t1_spzf-8+t2_kpzf as jjyk,t1_spzf-8+t2_zgzf as zgyk,t1_spzf-8+t2_spzf as spyk
          |from ta3 left join ta4 on ta3.`代码`=ta4.stock_code and ta3.trade_date=ta4.t1_trade_date
          |where stock_code is not null
          |order by t1_zgzf desc""".stripMargin)

      // 执行删除操作
      try {
        // 通过 Spark 执行 SQL 删除语句
        // 编写 SQL 删除语句
        val deleteQuery = s"DELETE FROM fx003 WHERE t1_trade_date='$formattedDate'"
        MysqlTools.mysqlEx("fx003",deleteQuery)
        println("数据删除成功！")
      } catch {
        case e: Exception => println(s"数据删除失败: ${e.getMessage}")
      }

      resdf.write.mode("append").jdbc(url, s"fx003", properties)
//      resdf.show(1000)

      spark.catalog.dropTempView("ta1")
      spark.catalog.dropTempView("ta2")
      spark.catalog.dropTempView("ta3")
      spark.catalog.dropTempView("ta4")
    }







    spark.close()
  }


}
