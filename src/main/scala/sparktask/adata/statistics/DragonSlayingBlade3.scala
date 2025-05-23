package sparktask.adata.statistics

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import sparktask.tools.MysqlTools
import org.apache.spark.sql.functions.col

import scala.collection.mutable.ArrayBuffer

/**
 * 屠龙刀战法 测评版本
 */
object DragonSlayingBlade3 {
  def main(args: Array[String]): Unit = {
    val startm = System.currentTimeMillis()
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "20")
      .set("spark.sql.broadcastTimeout","60000")
      .set("spark.driver.memory", "8g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "20")
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
//    val setdate = "20250307"
//    val formattedDate = s"${setdate.take(4)}-${setdate.slice(4, 6)}-${setdate.takeRight(2)}"

    val jyrldf: DataFrame = spark.read.jdbc(url, "data_jyrl", properties)
    jyrldf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    jyrldf.createOrReplaceTempView("data_jyrl")

    var ztbdf: DataFrame = spark.read.jdbc(url, s"ztb_day", properties)
    ztbdf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    ztbdf.createOrReplaceTempView("ztb_day")

    val columnsList = ArrayBuffer[String]("stock_code","t0_trade_date","t0_kxzt","t0_qjzf","t0_sfzt","t0_cjzt","t1_spzf","t2_spzf","t3_spzf","t4_spzf","t5_spzf","t6_spzf","t7_spzf","t8_spzf","t9_spzf","t10_spzf","t11_spzf","t12_spzf","t13_spzf","t14_spzf","t15_spzf","t1_trade_date","t2_trade_date","t3_trade_date","t4_trade_date","t5_trade_date","t6_trade_date","t7_trade_date","t8_trade_date","t9_trade_date","t10_trade_date","t11_trade_date","t12_trade_date","t13_trade_date","t14_trade_date","t15_trade_date")
    val data20_df = spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_20days\\trade_date_month=20[17,18,19,20,21,22,23,24,25]*")//
        .select(columnsList.map(col): _*)
    data20_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    data20_df.createOrReplaceTempView("data20_df")
    //    data20_df.show()

    val ylzc_before_df = spark.read.parquet("file:///D:\\gsdata\\pressure_support_calculator\\valid_results_pressure_advanced_dip_strategy")
//        .where("trade_time between '2020-01-01' and '2022-01-01'")
    ylzc_before_df.persist(StorageLevel.MEMORY_AND_DISK_SER)

//    val listsetDate = spark.sql("select trade_date from data_jyrl where  trade_date between '2020-01-01' and '2020-04-29' and trade_status='1' order by trade_date desc")
    val listsetDate = spark.sql("select trade_date from data_jyrl where  trade_date between '2018-01-01' and '2025-03-11' and trade_status='1' order by trade_date desc")
      .collect()
      .map(f => f.getAs[String]("trade_date"))

    for(setday<-listsetDate){
      val setdate = setday.replaceAll("-", "")
      val formattedDate = setday
      var querydf: DataFrame = spark.read.jdbc(url, s"wencaiquery_basequery_$setdate", properties).select("`代码`", "`简称`")
//      println("querydf-----------"+querydf.count())
      querydf.createOrReplaceTempView("basequery")

      //周期
      val cycledfArray = spark.sql(s"select trade_date from data_jyrl where trade_status=1 and trade_date<'$formattedDate' group by trade_date order by trade_date desc limit 200")
        .collect().map(f => f.getAs[String]("trade_date"))
      val beforedate = cycledfArray(0)
      val before2date = cycledfArray(2)
      val before15date = cycledfArray(15)
      val before120date = cycledfArray(119)
      println("当前日期:" + setdate + "\n上一个交易日：" + beforedate + "\n涨停筛选区间：" + before15date + "到" + before2date)


      //过滤出近15天有涨停的数据
      val zt15df = spark.sql(s"select `代码` as dm from basequery where `代码` in (select `股票代码`  from (select `股票代码`,trade_date,row_number() over(partition by `股票代码` order by `股票代码`,trade_date desc) as num from ztb_day where trade_date between  '$before15date' and '$before2date' and `连续涨停天数`<=2 ) where num=1)")
      zt15df.createOrReplaceTempView("zt15df")

      //屠龙刀条件1
      val tld_df1 = spark.sql("select `代码`,`简称`,'屠龙刀条件1' as `风险类型` from basequery left join zt15df on basequery.`代码`=zt15df.dm where dm is null")
//      println("1-------------"+tld_df1.count())

      //近15天有涨停且获取最近的涨停时间
      val dmrq_df = spark.sql(s"select * from (select `股票代码` as dm,trade_date,row_number() over(partition by `股票代码` order by `股票代码`,trade_date desc) as num from ztb_day where trade_date between  '$before15date' and '$before2date') where num=1")
//      dmrq_df.show(100)
      dmrq_df.createOrReplaceTempView("dmrq_df")


      /**
       * 爆头数据集过滤
       * 区间涨幅大于5的上影线
       * 区间涨幅大于5
       * 区间涨幅大于5的上影线/区间涨幅大于5 的比例
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
      bt_df.createOrReplaceTempView("bt_df")
//          bt_df.orderBy(col("pj_bfb").desc).show(5000)
      //屠龙刀条件2
      val tld_df2 = spark.sql(
        """select `代码`,`简称`,'屠龙刀条件2' as `风险类型` from basequery left join bt_df on basequery.`代码`=bt_df.stock_code where stock_code is null""".stripMargin)
//      println("2-------------"+tld_df2.count())

      //下跌df，最近的涨停板的涨停位置下跌8%
      val xd_df = spark.sql(
        s"""select dmrq_df.*,
          |if(t1_trade_date<='$beforedate',coalesce(t1_spzf,0),0) +
          |if(t2_trade_date<='$beforedate',coalesce(t2_spzf,0),0) +
          |if(t3_trade_date<='$beforedate',coalesce(t3_spzf,0),0) +
          |if(t4_trade_date<='$beforedate',coalesce(t4_spzf,0),0) +
          |if(t5_trade_date<='$beforedate',coalesce(t5_spzf,0),0) +
          |if(t6_trade_date<='$beforedate',coalesce(t6_spzf,0),0) +
          |if(t7_trade_date<='$beforedate',coalesce(t7_spzf,0),0) +
          |if(t8_trade_date<='$beforedate',coalesce(t8_spzf,0),0) +
          |if(t9_trade_date<='$beforedate',coalesce(t9_spzf,0),0) +
          |if(t10_trade_date<='$beforedate',coalesce(t10_spzf,0),0) +
          |if(t11_trade_date<='$beforedate',coalesce(t11_spzf,0),0) +
          |if(t12_trade_date<='$beforedate',coalesce(t12_spzf,0),0) +
          |if(t13_trade_date<='$beforedate',coalesce(t13_spzf,0),0) +
          |if(t14_trade_date<='$beforedate',coalesce(t14_spzf,0),0) +
          |if(t15_trade_date<='$beforedate',coalesce(t15_spzf,0),0)  as spzfadd
          |from data20_df left join dmrq_df on dmrq_df.dm=data20_df.stock_code and dmrq_df.trade_date=data20_df.t0_trade_date where dm in (select stock_code from bt_df)
          |""".stripMargin)
//      xd_df.orderBy("spzfadd").show(1000)
      xd_df.createOrReplaceTempView("xd_df")

      //屠龙刀条件3
      val tld_df3 = spark.sql("select `代码`,`简称`,'屠龙刀条件3' as `风险类型` from basequery left join xd_df on basequery.`代码`=xd_df.dm where spzfadd>=-6")
//      tld_df3.distinct().show(1000)
//      println("3-------------"+tld_df3.count())

      //过滤高标（开始向下） 压力支撑_df
      val ylzc_df = ylzc_before_df
        .where(s"trade_time='$beforedate'")

      val gb_df = querydf.join(ylzc_df,querydf("`代码`") === ylzc_df("stock_code")).distinct()
        .selectExpr("`代码` as dm","support_ratio","pressure_ratio","channel_position","support_types","pressure_types","windowSize")
        .orderBy("support_ratio")
      gb_df.createOrReplaceTempView("gb_df")
//      gb_df.show(1000)
      //屠龙刀条件4
      val tld_df4 = spark.sql("select `代码`,`简称`,'屠龙刀条件4' as `风险类型` from basequery left join gb_df on basequery.`代码`=gb_df.dm where (support_ratio>=150 or support_ratio<0)")
//      tld_df4.distinct().show(1000)
//      println("4-------------"+tld_df4.count())

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
      val tld_df = tld_df1.distinct().union(tld_df2.distinct()).union(tld_df3.distinct()).union(tld_df4.distinct())
      tld_df.write.mode("append").jdbc(url, s"wencaiquery_venture_$setdate", properties)

      //加载风险数据
      var venturedf: DataFrame = spark.read.jdbc(url, s"wencaiquery_venture_$setdate", properties).where("`风险类型`!='靠近压力位'")
//      venturedf.select("`风险类型`").distinct().show(100)
      venturedf.createOrReplaceTempView("venture")

      //过滤风险数据
      val glfx_df = spark.sql(s"select basequery.`代码` as dm, basequery.`简称`,fxlx,'$formattedDate' as trade_date from basequery left join (select `代码`, `简称`,concat_ws(',',collect_set(`风险类型`)) as fxlx from venture group by `代码`, `简称`) as venture on basequery.`代码`=venture.`代码` where fxlx is null").distinct()
      glfx_df.createOrReplaceTempView("glfx_df")

      //删除日期内的条件数据并重写
      try {
        // 通过 Spark 执行 SQL 删除语句
        val deleteQuery = s"DELETE FROM tld_filter WHERE trade_date='$formattedDate'"
        MysqlTools.mysqlEx("tld_filter", deleteQuery)
        println("数据删除成功！")
      } catch {
        case e: Exception => println(s"数据删除失败: ${e.getMessage}")
      }

      glfx_df.write.mode("append").jdbc(url, "tld_filter", properties)

      //风险数据观测
//      val fxdata_df = spark.sql("""select basequery.`代码` as dm, basequery.`简称`,fxlx from basequery left join (select `代码`, `简称`,collect_set(`风险类型`) as fxlx from venture group by `代码`, `简称`) as venture on basequery.`代码`=venture.`代码` where fxlx is not null""")
//        .distinct()
////      fxdata_df.show(1000,false)
//      fxdata_df.createOrReplaceTempView("fxdata_df")
//      val fx_df = spark.sql("select fx,count(1) as num from fxdata_df lateral view explode(fxlx) as fx group by fx order by fx")
////      fx_df.show(100)
//
//      glfx_df.select("dm").repartition(1).write.mode("overwrite").option("encoding", "UTF-8")
//        .text(s"file:///C:\\Users\\Administrator\\Desktop\\gs2024\\filter_table\\$setdate\\过滤风险")
//
//      spark.sql("select basequery.`代码` as dm from basequery left join glfx_df on basequery.`代码`=glfx_df.dm where dm is null").distinct()
//        .repartition(1).write.mode("overwrite").option("encoding", "UTF-8")
//        .text(s"file:///C:\\Users\\Administrator\\Desktop\\gs2024\\filter_table\\$setdate\\有风险dm")
    }

    jyrldf.unpersist()
    ztbdf.unpersist()
    data20_df.unpersist()
    ylzc_before_df.unpersist()

    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }


}
