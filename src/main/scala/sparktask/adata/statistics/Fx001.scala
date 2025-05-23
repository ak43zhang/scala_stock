package sparktask.adata.statistics

import java.util
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap

/**
 * 输出靠近压力位的风险数据到mysql风险表，并筛选出没有风险的数据和有风险的数据保存成txt文件
 */
object Fx001 {
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

    /**
     *  设置时间为处理当天，完成的数据为前一个交易日处理的数据。
     *  例如需要做的是2025-02-14，那么需要前一天2025-02-13数据获取风险数据进行过滤
     */
    val setdate = "20250305"
    val formattedDate = s"${setdate.take(4)}-${setdate.slice(4, 6)}-${setdate.takeRight(2)}"

    var df:DataFrame = spark.read.jdbc(url, s"wencaiquery_basequery_$setdate", properties).select("`代码`","`简称`")
    df.createTempView("ta1")

    val df3 = spark.read.parquet("file:///D:\\gsdata\\pressure_support_calculator\\valid_results_pressure_advanced_dip_strategy")
    df3.createTempView("ta3")
    // 找出当前设置时间的前一个交易时间
    val beforedate:String = spark.sql(s"select trade_time from ta3 where trade_time<'$formattedDate' group by trade_time order by trade_time desc limit 1").head().getString(0)

    // 筛选出最大时间对应的数据
    val df4 = spark.sql(s"select stock_code,trade_time,channel_position,windowSize,'靠近压力位' as `风险类型` from ta3 where trade_time='$beforedate' and (support_ratio>150 or  channel_position=101) ")
    df4.createTempView("ta4")


    //靠近压力位风险数据写入mysql
    val fxdf = spark.sql("select ta1.`代码`, ta1.`简称`,ta4.`风险类型` from ta1 left join ta4 on ta1.`代码`=ta4.stock_code where ta4.stock_code is not null")
    fxdf.write.mode("append").jdbc(url, s"wencaiquery_venture_$setdate", properties)

    var df2:DataFrame = spark.read.jdbc(url, s"wencaiquery_venture_$setdate", properties)
    df2.createTempView("ta2")


    val resdf = spark.sql("select ta1.*,ta2.fxlx from ta1 left join (select `代码`, `简称`,concat_ws(',',collect_set(`风险类型`)) as fxlx from ta2 group by `代码`, `简称`) as ta2 on ta1.`代码`=ta2.`代码` ")
    resdf.select("`代码`").where("fxlx is null").repartition(1).write.mode("overwrite").option("encoding", "UTF-8")
      .text(s"file:///C:\\Users\\Administrator\\Desktop\\gs2024\\filter_table\\$setdate\\过滤风险")

    resdf.where("fxlx is not null").repartition(1).write.mode("overwrite").option("encoding", "UTF-8")
      .csv(s"file:///C:\\Users\\Administrator\\Desktop\\gs2024\\filter_table\\$setdate\\有风险")

    resdf.select("`代码`").where("fxlx is not null").repartition(1).write.mode("overwrite").option("encoding", "UTF-8")
      .text(s"file:///C:\\Users\\Administrator\\Desktop\\gs2024\\filter_table\\$setdate\\有风险dm")




    spark.close()
  }


}
