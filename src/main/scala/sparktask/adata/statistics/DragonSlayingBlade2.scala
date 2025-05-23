package sparktask.adata.statistics

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import sparktask.tools.MysqlTools

/**
 * 屠龙刀战法
 */
object DragonSlayingBlade2 {
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
    val setdate = "20250307"
    val formattedDate = s"${setdate.take(4)}-${setdate.slice(4, 6)}-${setdate.takeRight(2)}"

    var querydf: DataFrame = spark.read.jdbc(url, s"wencaiquery_basequery_$setdate", properties).select("`代码`", "`简称`")
    println("querydf-----------"+querydf.count())
    querydf.createTempView("basequery")

    val jyrldf: DataFrame = spark.read.jdbc(url, "data_jyrl", properties)
    jyrldf.createOrReplaceTempView("data_jyrl")

    var ztbdf: DataFrame = spark.read.jdbc(url, s"ztb_day", properties)
    ztbdf.createTempView("ztb_day")

    //周期
    val cycledfArray = spark.sql(s"select trade_date from data_jyrl where trade_status=1 and trade_date<'$formattedDate' group by trade_date order by trade_date desc limit 200")
      .collect().map(f => f.getAs[String]("trade_date"))
    val beforedate = cycledfArray(0)
    val before2date = cycledfArray(2)
    val before15date = cycledfArray(15)
    val before120date = cycledfArray(119)
    println("当前日期:" + setdate + "\n上一个交易日：" + beforedate + "\n涨停筛选区间：" + before15date + "到" + before2date)


    //过滤出近15天有涨停的数据
    val zt15df = spark.sql(s"select `代码` as dm from basequery where `代码` in (select `股票代码`  from (select `股票代码`,trade_date,row_number() over(partition by `股票代码` order by `股票代码`,trade_date desc) as num from ztb_day where trade_date between  '$before15date' and '$before2date' and `连续涨停天数`<=2) where num=1)")
    zt15df.createTempView("zt15df")

    //屠龙刀条件1
    val tld_df1 = spark.sql("select `代码`,`简称`,'屠龙刀条件1' as `风险类型` from basequery left join zt15df on basequery.`代码`=zt15df.dm where dm is null")
    println("1-------------"+tld_df1.count())

    //近15天有涨停且获取最近的涨停时间
    val dmrq_df = spark.sql(s"select * from (select `股票代码` as dm,trade_date,row_number() over(partition by `股票代码` order by `股票代码`,trade_date desc) as num from ztb_day where trade_date between  '$before15date' and '$before2date') where num=1")
    dmrq_df.createTempView("dmrq_df")


    val data20_df = spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_20days\\trade_date_month=202[4,5]*")
    data20_df.createTempView("data20_df")
//    data20_df.show()

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
         |from data20_df
         |where t0_trade_date between  '$before120date' and '$before2date'
         |group by stock_code""".stripMargin).where("zts>=zbs and zts>=5 and pj_bfb<=0.3")
      .select("stock_code")
    bt_df.createTempView("bt_df")
    //    bt_df.orderBy(col("pj_bfb").desc).show(5000)
    //屠龙刀条件2
    val tld_df2 = spark.sql("select `代码`,`简称`,'屠龙刀条件2' as `风险类型` from basequery left join bt_df on basequery.`代码`=bt_df.stock_code where stock_code is null")
    println("2-------------"+tld_df2.count())

    //下跌df，最近的涨停板的涨停位置下跌6%
    val xd_df = spark.sql(
      """select dmrq_df.*,coalesce(t1_spzf,0)+coalesce(t2_spzf,0)+coalesce(t3_spzf,0)+coalesce(t4_spzf,0)+coalesce(t5_spzf,0)+coalesce(t6_spzf,0)+coalesce(t7_spzf,0)+coalesce(t8_spzf,0)+coalesce(t9_spzf,0)+coalesce(t10_spzf,0)+coalesce(t11_spzf,0)+coalesce(t12_spzf,0)+coalesce(t13_spzf,0)+coalesce(t14_spzf,0)+coalesce(t15_spzf,0) as spzfadd
        |from dmrq_df left join data20_df on dmrq_df.dm=data20_df.stock_code and dmrq_df.trade_date=data20_df.t0_trade_date where dm in (select * from bt_df)
        |""".stripMargin).where("spzfadd<-6")
    //    resdf.orderBy("spzfadd").show(100)
    xd_df.createTempView("xd_df")

    //屠龙刀条件3
    val tld_df3 = spark.sql("select `代码`,`简称`,'屠龙刀条件3' as `风险类型` from basequery left join xd_df on basequery.`代码`=xd_df.dm where dm is null")
    println("3-------------"+tld_df3.count())

    //TODO 过滤高标（开始向下） 压力支撑_df
    val ylzc_df = spark.read.parquet("file:///D:\\gsdata\\pressure_support_calculator\\valid_results_pressure_advanced_dip_strategy")
      .where(s"trade_time='$beforedate'")

    val gb_df = xd_df.join(ylzc_df,xd_df("dm") === ylzc_df("stock_code")).distinct()
      .where("support_ratio<=150")
      .select("dm","spzfadd","support_ratio","pressure_ratio","channel_position","support_types","pressure_types","windowSize")
      .orderBy("support_ratio")
    gb_df.createTempView("gb_df")
    gb_df.show()
    //屠龙刀条件4
    val tld_df4 = spark.sql("select `代码`,`简称`,'屠龙刀条件4' as `风险类型` from basequery left join gb_df on basequery.`代码`=gb_df.dm where dm is null")
    println("4-------------"+tld_df4.count())

    //删除屠龙刀条件数据
    try {
      // 通过 Spark 执行 SQL 删除语句
      // 编写 SQL 删除语句
      val deleteQuery = s"DELETE FROM wencaiquery_venture_$setdate WHERE `风险类型` like '屠龙刀条件%'"
      MysqlTools.mysqlEx(s"wencaiquery_venture_$setdate", deleteQuery)
      println("数据删除成功！")
    } catch {
      case e: Exception => println(s"数据删除失败: ${e.getMessage}")
    }
    //将屠龙刀条件风险数据写入风险数据库
    val tld_df = tld_df1.union(tld_df2).union(tld_df3).union(tld_df4)
    tld_df.write.mode("append").jdbc(url, s"wencaiquery_venture_$setdate", properties)

    var venturedf: DataFrame = spark.read.jdbc(url, s"wencaiquery_venture_$setdate", properties)
    venturedf.createTempView("venture")

    //过滤风险数据
    val glfx_df = spark.sql("select basequery.`代码` as dm, basequery.`简称`,fxlx from basequery left join (select `代码`, `简称`,concat_ws(',',collect_set(`风险类型`)) as fxlx from venture group by `代码`, `简称`) as venture on basequery.`代码`=venture.`代码` where fxlx is null")
    glfx_df.show(100)
    glfx_df.createTempView("glfx_df")

    //风险数据观测
    val fxdata_df = spark.sql("select basequery.`代码` as dm, basequery.`简称`,fxlx from basequery left join (select `代码`, `简称`,collect_set(`风险类型`) as fxlx from venture group by `代码`, `简称`) as venture on basequery.`代码`=venture.`代码` where fxlx is not null")
        .distinct()
    fxdata_df.show(1000,false)
    fxdata_df.createTempView("fxdata_df")
    spark.sql("select fx,count(1) as num from fxdata_df lateral view explode(fxlx) as fx group by fx order by fx").show(100)

    glfx_df.select("dm").repartition(1).write.mode("overwrite").option("encoding", "UTF-8")
      .text(s"file:///C:\\Users\\Administrator\\Desktop\\gs2024\\filter_table\\$setdate\\过滤风险")

    spark.sql("select basequery.`代码` as dm from basequery left join glfx_df on basequery.`代码`=glfx_df.dm where dm is null").distinct()
      .repartition(1).write.mode("overwrite").option("encoding", "UTF-8")
      .text(s"file:///C:\\Users\\Administrator\\Desktop\\gs2024\\filter_table\\$setdate\\有风险dm")



    spark.close()
  }


}
