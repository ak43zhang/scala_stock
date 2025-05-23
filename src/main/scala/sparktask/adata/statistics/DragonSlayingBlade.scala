package sparktask.adata.statistics

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 屠龙刀战法
 */
object DragonSlayingBlade {
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
     * setdate：设置当天盯盘时间
     * beforedate：取盯盘前一交易日
     * before10date:取盯盘前10个交易日
     */
    val setdate = "20250306"
    val formattedDate = s"${setdate.take(4)}-${setdate.slice(4, 6)}-${setdate.takeRight(2)}"

    var querydf: DataFrame = spark.read.jdbc(url, s"wencaiquery_basequery_$setdate", properties).select("`代码`", "`简称`")
    querydf.createTempView("ta1")

    var venturedf: DataFrame = spark.read.jdbc(url, s"wencaiquery_venture_$setdate", properties)
    venturedf.createTempView("ta2")

    val jyrldf: DataFrame = spark.read.jdbc(url, "data_jyrl", properties)
    jyrldf.createOrReplaceTempView("ta3")

    var ztbdf: DataFrame = spark.read.jdbc(url, s"ztb_day", properties)
    ztbdf.createTempView("ta4")

    //周期
    val cycledfArray = spark.sql(s"select trade_date from ta3 where trade_status=1 and trade_date<'$formattedDate' group by trade_date order by trade_date desc limit 200")
      .collect().map(f => f.getAs[String]("trade_date"))
    val beforedate = cycledfArray(0)
    val before2date = cycledfArray(2)
    val before15date = cycledfArray(15)
    val before120date = cycledfArray(119)
    println("当前日期:" + setdate + "\n上一个交易日：" + beforedate + "\n涨停筛选区间：" + before15date + "到" + before2date)


    //过滤出近15天有涨停的数据
    val zt15df = spark.sql(s"select `代码` as dm from ta1 where `代码` in (select `股票代码`  from (select `股票代码`,trade_date,row_number() over(partition by `股票代码` order by `股票代码`,trade_date desc) as num from ta4 where trade_date between  '$before15date' and '$before2date' and `连续涨停天数`<=2) where num=1)")
    zt15df.createTempView("ta5")


    //近15天有涨停且获取最近的涨停时间
    val dmrq_df = spark.sql(s"select * from (select `股票代码` as dm,trade_date,row_number() over(partition by `股票代码` order by `股票代码`,trade_date desc) as num from ta4 where trade_date between  '$before15date' and '$before2date') where num=1")
    dmrq_df.createTempView("ta6")

    //过滤风险数据
    val glfx_df = spark.sql("select dm from ta5 left join (select `代码`, `简称`,concat_ws(',',collect_set(`风险类型`)) as fxlx from ta2 group by `代码`, `简称`) as ta2 on ta5.dm=ta2.`代码` where fxlx is null")
    glfx_df.createTempView("ta7")

    //筛选最终数据
    spark.sql("select ta6.* from ta7 left join ta6 on ta7.dm = ta6.dm").createTempView("ta8")

    val data20_df = spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_20days\\trade_date_month=202[4,5]*")
    data20_df.createTempView("ta9")
//        data20_df.show()

    /**
     * 爆头数据集过滤
     * 实体涨幅大于5的上影线
     * 实体涨幅大于5
     * 实体涨幅大于5的上影线/实体涨幅大于5 的比例
     * 涨停板数量
     * 炸板数量
     */
    val bt_df = spark.sql(
      s"""select stock_code,
         |sum(if(t0_kxzt like '%上影线%' and t0_qjzf>5,1,0)) as syx_count,
         |sum(if(t0_qjzf>5,1,0)) as sl,
         |round(sum(if(t0_kxzt like '%上影线%' and t0_qjzf>5,1,0))/sum(if(t0_qjzf>5,1,0)),2) as syx_bfb,
         |sum(if(t0_sfzt=1,1,0)) as zts,
         |sum(if(t0_cjzt=1,1,0)) as zbs,
         |round(sum(if(t0_cjzt=1,1,0))/sum(if(t0_sfzt=1 or t0_cjzt=1,1,0)),2) as zb_bfb,
         |(round(sum(if(t0_kxzt like '%上影线%' and t0_qjzf>5,1,0))/sum(if(t0_qjzf>5,1,0)),2)+round(sum(if(t0_cjzt=1,1,0))/sum(if(t0_sfzt=1 or t0_cjzt=1,1,0)),2))/2 as pj_bfb
         |from ta9
         |where t0_trade_date between  '$before120date' and '$before2date'
         |group by stock_code""".stripMargin).where("zts>=zbs and zts>=5 and pj_bfb<=0.3")
      .select("stock_code")
    bt_df.createTempView("ta11")
//    bt_df.orderBy(col("pj_bfb").desc).show(5000)


    //if(t1_kxzt like '%上影线%',1,0)+if(t2_kxzt like '%上影线%',1,0)+if(t3_kxzt like '%上影线%',1,0)+if(t4_kxzt like '%上影线%',1,0)+if(t5_kxzt like '%上影线%',1,0)+if(t6_kxzt like '%上影线%',1,0)+if(t7_kxzt like '%上影线%',1,0)+if(t8_kxzt like '%上影线%',1,0)+if(t9_kxzt like '%上影线%',1,0)+if(t10_kxzt like '%上影线%',1,0)+if(t11_kxzt like '%上影线%',1,0)+if(t12_kxzt like '%上影线%',1,0)+if(t13_kxzt like '%上影线%',1,0)+if(t14_kxzt like '%上影线%',1,0)+if(t15_kxzt like '%上影线%',1,0) as syx_count
    val resdf = spark.sql(
      """select ta8.*,coalesce(t1_spzf,0)+coalesce(t2_spzf,0)+coalesce(t3_spzf,0)+coalesce(t4_spzf,0)+coalesce(t5_spzf,0)+coalesce(t6_spzf,0)+coalesce(t7_spzf,0)+coalesce(t8_spzf,0)+coalesce(t9_spzf,0)+coalesce(t10_spzf,0)+coalesce(t11_spzf,0)+coalesce(t12_spzf,0)+coalesce(t13_spzf,0)+coalesce(t14_spzf,0)+coalesce(t15_spzf,0) as spzfadd
        |from ta8 left join ta9 on ta8.dm=ta9.stock_code and ta8.trade_date=ta9.t0_trade_date where dm in (select * from ta11)
        |""".stripMargin).where("spzfadd<-6")
//    resdf.orderBy("spzfadd").show(100)
    resdf.createTempView("ta10")

    //TODO 过滤高标（开始向下）
    val ylzc_df = spark.read.parquet("file:///D:\\gsdata\\pressure_support_calculator\\valid_results_pressure_advanced_dip_strategy")
      .where(s"trade_time='$beforedate' and support_ratio<=150")

    val lastresdf = resdf.join(ylzc_df,resdf("dm") === ylzc_df("stock_code")).distinct()
      .select("dm","spzfadd","support_ratio","pressure_ratio","channel_position","support_types","pressure_types","windowSize")
      .orderBy("support_ratio")
    lastresdf.createTempView("ta12")
    lastresdf.show(100)

    lastresdf.select("dm").repartition(1).write.mode("overwrite").option("encoding", "UTF-8")
      .text(s"file:///C:\\Users\\Administrator\\Desktop\\gs2024\\filter_table\\$setdate\\过滤风险")

    spark.sql("select `代码` as dm from ta1 left join ta12 on ta1.`代码`=ta12.dm where dm is null").distinct()
      .repartition(1).write.mode("overwrite").option("encoding", "UTF-8")
      .text(s"file:///C:\\Users\\Administrator\\Desktop\\gs2024\\filter_table\\$setdate\\有风险dm")



    spark.close()
  }


}
