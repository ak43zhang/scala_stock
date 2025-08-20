package sparktask.adata.dwd.v2

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import sparktask.tools.MysqlTools

import scala.collection.mutable.ArrayBuffer

object ImportData2THS_v2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "200")
      .set("spark.driver.memory", "8g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "10")
      .set("spark.sql.parquet.enableVectorizedReader","false")
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

//    val setdate = "2025-04-15"
//    val setdate_ = setdate.replaceAll("-","")
//    val year = setdate.substring(0,4)

    val start_time ="2025-08-20"
    val end_time ="2025-08-20"
    
    val r1_table = "result_expect_risk1"
    val r2_table = "result_expect_risk2"
    val n1_table = "result_expect_normal"

    val columnsList = ArrayBuffer[String]("stock_code","t1_trade_date","t2_trade_date","t3_trade_date",
      "t0_sfzt","t0_cjzt","t0_kxzt","t0_ln","t0_zrlnb","t0_qjzf","t0_stzf","t0_kpzf","t0_zgzf","t0_zdzf","t0_spzf",
      "t1_sfzt","t1_cjzt","t1_kxzt","t1_ln","t1_zrlnb","t1_qjzf","t1_stzf","t1_kpzf","t1_zgzf","t1_zdzf","t1_spzf",
      "t2_sfzt","t2_cjzt","t2_kxzt","t2_ln","t2_zrlnb","t2_qjzf","t2_stzf","t2_kpzf","t2_zgzf","t2_zdzf","t2_spzf"
      )//"t3_sfzt","t3_cjzt","t3_kxzt","t3_ln","t3_zrlnb","t3_qjzf","t3_stzf","t3_kpzf","t3_zgzf","t3_zdzf","t3_spzf"
    val data20_df = spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_10days\\trade_date_month=20[25]*") //18,19,20,21,22,23,24
      .select(columnsList.map(col): _*)
    data20_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    data20_df.createOrReplaceTempView("data20_df")

    //压力支撑数据
    val ps_df = spark.read.parquet("file:///D:\\gsdata\\pressure_support_calculator\\valid_results_pressure_advanced_dip_strategy")
      .select("stock_code","trade_time","channel_position","support_ratio","pressure_ratio")
//        .where(s"trade_time between '$start_time' and '$end_time'")
    ps_df.persist(StorageLevel.MEMORY_AND_DISK_SER)

    //通达信风险数据
    val risk_tdx_df: DataFrame = spark.read.jdbc(url, "data_risk_tdx", properties)
    risk_tdx_df.createOrReplaceTempView("data_risk_tdx")

    val ztb_df: DataFrame = spark.read.jdbc(url, "ztb_day", properties)
    ztb_df.createOrReplaceTempView("ztb")

    val jyrls = spark.read.jdbc(url, "data_jyrl", properties)
      .where(s"trade_status=1 and trade_date between '$start_time' and '$end_time'")
      .orderBy(col("trade_date").desc)
      .select("trade_date").collect().map(f => f.getAs[String]("trade_date")).toList
    for (jyrl <- jyrls) {
      val startm = System.currentTimeMillis()

      val setdate = jyrl
      val date_list = spark.read.jdbc(url, "data_jyrl", properties).orderBy(col("trade_date").desc)
        .where(s"trade_status=1 and trade_date<'$jyrl'").collect().map(f => f.getAs[String]("trade_date"))
      val yes_day = date_list.toList(0)
      val n_day_ago = date_list.toList(6)
      val setdate_ = setdate.replaceAll("-","")
      val year = setdate.substring(0,4)
      println("==========================设置时间============================"+setdate)
      println(s"风险时间:$yes_day============================5个交易日前：$n_day_ago")

      var basequerydf: DataFrame = spark.read.jdbc(url, s"wencaiquery_basequery_$year", properties)
        .where(s"trade_date='$setdate' ")
        .select("`代码`", "`简称`")
      println("querydf-----------"+basequerydf.count())
      basequerydf.createOrReplaceTempView("basequery")

      var venturedf: DataFrame = spark.read.jdbc(url, s"wencaiquery_venture_$year", properties)
        .where(s"trade_date='$setdate'")
      println("venturedf-----------"+venturedf.count())
      venturedf.createOrReplaceTempView("venture")

      var venture_year_df: DataFrame = spark.read.jdbc(url, s"wencaiquery_venture_year", properties)
        .where(s"year='$year'")
      println("venture_year_df-----------"+venture_year_df.count())
      venture_year_df.createOrReplaceTempView("venture_year")

      val ps2_df = ps_df.where(s"trade_time='$yes_day'")
      ps2_df.createOrReplaceTempView("ps")

      //风险数据sql+通达信风险数据
      //暂时未加入：业绩公告预警,累计经营现金流偏少,关联交易风险,高商誉风险,公司被监管警示,大股东离婚,子公司风险,公允价值收益异常,主力资金卖出,交易所监管,行政监管措施或处罚,公司被立案调查,高层协助有关机构调查,股东或子公司预重整,市盈率过高,募投项目延期,A股溢价过高,分析师评级下调,股价脚踝斩,拟大比例减持,可转债临近转股期,市净率过高,停牌次数多,十大股东总持股占比小,关联人员市场禁入,公司预重整,多元化经营风险,上市首日遭爆炒,年营收偏低,可能被*ST,会计师事务所变动,审计报告其他非标,净资产可能低于亏损,审计报告保留意见,分红不达标,定期报告存疑,*ST股票,财务造假,净资产偏低,资金占用或违规担保,股价偏低,可能终止上市,年报被问询,市值偏小,拟主动退市
      val risk_df = spark.sql(
        """
          |(select `代码`,`风险类型` from venture_year) union (select `代码`,`风险类型` from venture) union (select stock_code as `代码`,concat('risk_',s_type) as `风险类型` from data_risk_tdx where f_type = 'ST风险和退市' and s_type not in ('年营收偏低','会计师事务所变动','审计报告其他非标','审计报告保留意见')) union (select stock_code as `代码`,concat('risk_',s_type) as `风险类型` from data_risk_tdx where s_type = '财报亏损' and reason REGEXP '(?=.*亿)(?=.*2025)') union (select  stock_code as `代码`,concat('risk_',s_type) as `风险类型`  from data_risk_tdx where s_type in ('控股股东高质押','长期不分红' ,'业绩变脸风险'  ,'大比例解禁' ,'股权分散' ,'失信被执行' ,'事故或停产' ,'未能如期披露财报' ,'银行账户被冻结' ,'高层或股东被立案调查' ,'清仓式减持' ,'债券违约' ,'理财风险' ,'补充质押' ) ) union (select stock_code as `代码`,concat('risk_',s_type) as `风险类型` from data_risk_tdx where s_type in ('高管人员偏少','应收账款占收入比例过高','短期负债风险','高应收款隐忧','高财务费用','扣非净利润陡降风险','非经常性损益占比过高','高负债率','经营现金流风险','最大供应商采购占比高','业绩骤减','高担保比例','高质押风险','营收陡降风险','多年扣非亏损','投资收益巨亏','最大客户占营收比过高','应收款增加,低现金流','应付债券占比高','已大比例减持','高库存隐忧','高层涉刑','营业外支出过大','负债率逐年递增','员工人数陡降','高预付款隐忧','研发费用减少','研发人员减少','被调出重要指数','员工人数偏少','毛利率偏低','高销售费用','货币资产充足仍举债','存货同比大幅增加','交易异常监管','被频繁问询监管','采购暂停或市场禁入','负面舆情','交易所警示','三费占营收比例高','控制权纠纷或争斗','股权冻结') and reason REGEXP '(?=.*2025)')
          |""".stripMargin)
      risk_df.where("`风险类型` not in ('股东大会','预约披露时间_巨潮资讯')")
        .createOrReplaceTempView("venture2")

      val risk2_df = spark.sql(
        s"""
          |select dm,jc,'$setdate' as trade_date,'$yes_day' as yes_day from
          |(select q1.`代码` as dm,`简称` as jc,collect_list(`风险类型`) as fxlxs from basequery as q1 left join venture2 as q2 on q1.`代码`=q2.`代码`  group by q1.`代码`,`简称`)
          |where size(fxlxs)=0
          |""".stripMargin)
      println("========================================================================================ta1")
      risk2_df.show()
      risk2_df.createOrReplaceTempView("ta1")

      val fx1_df = spark.sql(
        s"""
           |select dm,jc,concat_ws(',',fxlxs) as fxlxs,'$setdate' as trade_date,'$yes_day' as yes_day from
           |(select q1.`代码` as dm,`简称` as jc,collect_list(`风险类型`) as fxlxs from basequery as q1 left join venture2 as q2 on q1.`代码`=q2.`代码`  group by q1.`代码`,`简称`)
           |where size(fxlxs)!=0
           |""".stripMargin)
      println("========================================================================================fx1_df")
      println("fx1_df-----------"+fx1_df.count())
      fx1_df.show(false)

      fx1_df.select("dm").repartition(1).write.mode("overwrite").option("encoding", "UTF-8")
        .text(s"file:///C:\\Users\\Administrator\\Desktop\\gs2024\\filter_table\\${setdate_}\\风险")

      try {
        // 通过 Spark 执行 SQL 删除语句
        // 编写 SQL 删除语句
        val deleteQuery = s"DELETE FROM $r1_table WHERE trade_date='$setdate'"
        MysqlTools.mysqlEx(s"$r1_table", deleteQuery)
        println("数据删除成功！")
      } catch {
        case e: Exception => println(s"数据删除失败: ${e.getMessage}")
      }

      fx1_df.withColumn("fxlxs", concat_ws(",", col("fxlxs")))
        .write.mode("append").jdbc(url,s"$r1_table",properties)


      //,t3_kpzf,t3_zgzf,t3_zdzf,t3_spzf    dm is not null and
      spark.sql(
        s"""
           |select dm,jc,t1_trade_date,t2_trade_date,t0_kxzt,t1_kxzt,t1_ln,t1_zrlnb,t0_spzf,t1_qjzf,t1_stzf,t1_kpzf,t1_zgzf,t1_zdzf,t1_spzf,t2_sfzt,t2_cjzt,t2_kpzf,t2_zgzf,t2_zdzf,t2_spzf
           |from ta1 left join (select * from data20_df where t1_trade_date='$yes_day') as d2 on d2.t1_trade_date = ta1.yes_day and d2.stock_code = ta1.dm
           |""".stripMargin).createOrReplaceTempView("ta2")

      val fx2_df = spark.sql(
        s"""
          |select *,'$setdate' as trade_date,
          |concat_ws(',',case when t1_qjzf>=6 then '1' end,
          |case when t1_zgzf>=3 then '2' end,
          |case when t1_kxzt='红柱上影线' then '3' end,
          |case when t1_zrlnb>=1.3 then '4' end,
          |case when t1_stzf>=3 then '5' end,
          |case when channel_position>=80 then '6' end ) as fxlx
          | from ta2 left join ps on ta2.t1_trade_date = ps.trade_time and ta2.dm = ps.stock_code
          |where t1_qjzf>=6 or t1_zgzf>=3 or t1_kxzt ='红柱上影线' or t1_zrlnb>=1.5 or t1_stzf>=3 or channel_position>=80
          |order by channel_position desc
          |""".stripMargin)
      println("========================================================================================fx2_df")
      println("fx2_df-----------"+fx2_df.count())
      fx2_df.show()
      fx2_df.createOrReplaceTempView("fx2_df")

      fx2_df.select("dm").repartition(1).write.mode("overwrite").option("encoding", "UTF-8")
        .text(s"file:///C:\\Users\\Administrator\\Desktop\\gs2024\\filter_table\\${setdate_}\\风险2")

      try {
        // 通过 Spark 执行 SQL 删除语句
        val deleteQuery = s"DELETE FROM $r2_table WHERE trade_date='$setdate'"
        MysqlTools.mysqlEx(s"$r2_table", deleteQuery)
        println("数据删除成功！")
      } catch {
        case e: Exception => println(s"数据删除失败: ${e.getMessage}")
      }

      fx2_df.select("dm","trade_date","fxlx").write.mode("append").jdbc(url,s"$r2_table",properties)

//      val nf_df = spark.sql(
//        s"""
//          |select dm,jc,'$setdate' as trade_date from ta2 left join ps on ta2.t1_trade_date=ps.trade_time and ta2.dm = ps.stock_code
//          |where t1_qjzf<6 and t1_zgzf<3 and t1_kxzt !='红柱上影线' and t1_zrlnb<1.3 and t1_stzf<3 and channel_position<80
//          |order by channel_position desc
//          |""".stripMargin)

      //
      val nf_df = spark.sql(
        s"""
           |select ta1.* from ta1 left join fx2_df on ta1.yes_day=fx2_df.t1_trade_date and ta1.dm = fx2_df.dm
           |where fx2_df.dm is null
           |""".stripMargin)

      println("========================================================================================nf_df")
      println("nf_df-----------"+nf_df.count())
      nf_df.show(1000)

      nf_df.select("dm").repartition(1).write.mode("overwrite").option("encoding", "UTF-8")
        .text(s"file:///C:\\Users\\Administrator\\Desktop\\gs2024\\filter_table\\${setdate_}\\关注")

      try {
        // 通过 Spark 执行 SQL 删除语句
        val deleteQuery = s"DELETE FROM $n1_table WHERE trade_date='$setdate'"
        MysqlTools.mysqlEx(s"$n1_table", deleteQuery)
        println("数据删除成功！")
      } catch {
        case e: Exception => println(s"数据删除失败: ${e.getMessage}")
      }

      nf_df.select("dm","trade_date").write.mode("append").jdbc(url,s"$n1_table",properties)

      nf_df.select("dm").createOrReplaceTempView("n1")

      spark.sql(
        s"""
          |select n1.dm from n1 left join (select * from ztb where trade_date between '$n_day_ago' and '$yes_day') as z on n1.dm=z.`股票代码` where z.`股票代码` is not null
          |""".stripMargin).select("dm").repartition(1).write.mode("overwrite").option("encoding", "UTF-8")
        .text(s"file:///C:\\Users\\Administrator\\Desktop\\gs2024\\filter_table\\${setdate_}\\关注7日内涨停")

      val endm = System.currentTimeMillis()
      println("共耗时：" + (endm - startm) / 1000 + "秒")
    }

    spark.close()
  }
}
