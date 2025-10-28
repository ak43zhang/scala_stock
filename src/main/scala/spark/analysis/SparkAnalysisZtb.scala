package spark.analysis

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import sparktask.tools.MysqlTools

import scala.collection.mutable.ArrayBuffer

/**
 * 阶段涨停板回撤选股法
 *
 * 定义选股日期：start_time——end_time 之间 setdate
 * risk_df————风险数据sql+通达信风险数据
 * 1、分析最近涨停板（涨停板上）与当前交易日的上一交易日（yes_day）的涨跌幅差,再通过压力位支撑位确定位置，排除不在模式内的票
 *        涨跌幅差
 *        压力支撑位置
 * 2、风险关联
 *
 * 3、热度关联
 *
 *
 * dataframe
 * 涨停板                                         ztb_day（mysql）                                                ztb
 * 热度                                           popularity_day（mysql）                                         rd
 * 通达信风险数据                                   data_risk_tdx（mysql）                                         data_risk_tdx
 * 风险数据评分                                     metadata_risk_level（mysql）                                   mrl
 * 10日数据集                                      gpsj_hs_10days（parquet）                                      data10_df
 * 压力支撑数据[统一使用40日周期压力支撑位]             valid_results_pressure_advanced_dip_strategy40（parquet）       ps
 * 交易日历                                        data_jyrl（mysql）                                             data_jyrl
 * 基础查询数据                                     wencaiquery_basequery_$year（mysql）                           basequery
 * 风险数据                                        wencaiquery_venture_$year（mysql）                             venture
 * 年风险数据                                      wencaiquery_venture_year（mysql）                              venture_year
 *
 *
 *
 *
 *
 */
object SparkAnalysisZtb {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "10")
      .set("spark.sql.broadcastTimeout","60000")
      .set("spark.driver.memory", "6g")
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
    val startm = System.currentTimeMillis()

    val start_time = "2025-01-01"
    val end_time ="2025-10-27"

    //涨停板dataframe
    val ztb_df: DataFrame = spark.read.jdbc(url, "ztb_day", properties)
        .select("股票代码","股票简称","连续涨停天数","trade_date","analysis")
    ztb_df.createOrReplaceTempView("ztb")

    // 热度dataframe
    val rd_df: DataFrame = spark.read.jdbc(url, "popularity_day", properties)
    rd_df.createOrReplaceTempView("rd")

    //通达信风险数据
    val risk_tdx_df: DataFrame = spark.read.jdbc(url, "data_risk_tdx", properties)
    risk_tdx_df.createOrReplaceTempView("data_risk_tdx")

    //风险数据评分
    val metadata_risk_level_df: DataFrame = spark.read.jdbc(url, "metadata_risk_level", properties)
    metadata_risk_level_df.createOrReplaceTempView("mrl")

    val columnsList = ArrayBuffer[String]("stock_code","t0_trade_date","t1_trade_date","t2_trade_date","t3_trade_date",
      "t0_close","t0_sfzt","t0_cjzt","t0_kxzt","t0_ln","t0_zrlnb","t0_qjzf","t0_stzf","t0_kpzf","t0_zgzf","t0_zdzf","t0_spzf",
      "t1_close","t1_sfzt","t1_cjzt","t1_kxzt","t1_ln","t1_zrlnb","t1_qjzf","t1_stzf","t1_kpzf","t1_zgzf","t1_zdzf","t1_spzf",
      "t2_close","t2_sfzt","t2_cjzt","t2_kxzt","t2_ln","t2_zrlnb","t2_qjzf","t2_stzf","t2_kpzf","t2_zgzf","t2_zdzf","t2_spzf"
//      "t3_close","t3_sfzt","t3_cjzt","t3_kxzt","t3_ln","t3_zrlnb","t3_qjzf","t3_stzf","t3_kpzf","t3_zgzf","t3_zdzf","t3_spzf"
    )//"t3_sfzt","t3_cjzt","t3_kxzt","t3_ln","t3_zrlnb","t3_qjzf","t3_stzf","t3_kpzf","t3_zgzf","t3_zdzf","t3_spzf"
    val data10_df = spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_10days\\trade_date_month=20[23,24,25]*")//15,16,17,18,19,20,21,22,
      .select(columnsList.map(col): _*)
    data10_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    data10_df.createOrReplaceTempView("data10_df")

    //压力支撑数据[统一使用40日周期压力支撑位]
    val ps_df = spark.read.parquet("file:///D:\\gsdata\\pressure_support_calculator\\valid_results_pressure_advanced_dip_strategy40")
      .select("stock_code","trade_time","windowSize","pivot_pressure","pivot_support","high_low_pressure","high_low_support","channel_position","support_ratio","pressure_ratio")
//            .where(s"trade_time between '$start_time' and '$end_time'")
    ps_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
//    ps_df.show()
//    ps_df.printSchema()

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
      val yes_day_5 = date_list.toList(5)
      val n_day_ago = date_list.toList(20)
      val day_40_ago = date_list.toList(40)
//      println(n_day_ago)
      val year = setdate.substring(0,4)
      println(s"==========================设置时间为${setdate},则处理的是${setdate}的股票，分析股票则用上一交易日：${yes_day}============================")

      //获取需要处理的股票集合
      var basequerydf: DataFrame = spark.read.jdbc(url, s"wencaiquery_basequery_$year", properties)
        .where(s"trade_date='$setdate'")
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

      //风险数据sql+通达信风险数据
      //暂时未加入：业绩公告预警,累计经营现金流偏少,关联交易风险,高商誉风险,公司被监管警示,大股东离婚,子公司风险,公允价值收益异常,主力资金卖出,交易所监管,行政监管措施或处罚,公司被立案调查,高层协助有关机构调查,股东或子公司预重整,市盈率过高,募投项目延期,A股溢价过高,分析师评级下调,股价脚踝斩,拟大比例减持,可转债临近转股期,市净率过高,停牌次数多,十大股东总持股占比小,关联人员市场禁入,公司预重整,多元化经营风险,上市首日遭爆炒,年营收偏低,可能被*ST,会计师事务所变动,审计报告其他非标,净资产可能低于亏损,审计报告保留意见,分红不达标,定期报告存疑,*ST股票,财务造假,净资产偏低,资金占用或违规担保,股价偏低,可能终止上市,年报被问询,市值偏小,拟主动退市
      val risk_df = spark.sql(
        """
          |(select `代码`,`风险类型` from venture_year)
          |union
          |(select `代码`,`风险类型` from venture)
          |union
          |(select stock_code as `代码`,concat('risk_',s_type) as `风险类型` from data_risk_tdx where f_type = 'ST风险和退市' and s_type not in ('年营收偏低','会计师事务所变动','审计报告其他非标','审计报告保留意见'))
          |union
          |(select stock_code as `代码`,concat('risk_',s_type) as `风险类型` from data_risk_tdx where s_type = '财报亏损' and reason REGEXP '(?=.*亿)(?=.*2025)')
          |union
          |(select  stock_code as `代码`,concat('risk_',s_type) as `风险类型`  from data_risk_tdx where s_type in ('控股股东高质押','长期不分红' ,'业绩变脸风险'  ,'大比例解禁' ,'股权分散' ,'失信被执行' ,'事故或停产' ,'未能如期披露财报' ,'银行账户被冻结' ,'高层或股东被立案调查' ,'清仓式减持' ,'债券违约' ,'理财风险' ,'补充质押' ) )
          |union
          |(select stock_code as `代码`,concat('risk_',s_type) as `风险类型` from data_risk_tdx where s_type in ('高管人员偏少','应收账款占收入比例过高','短期负债风险','高应收款隐忧','高财务费用','扣非净利润陡降风险','非经常性损益占比过高','高负债率','经营现金流风险','最大供应商采购占比高','业绩骤减','高担保比例','高质押风险','营收陡降风险','多年扣非亏损','投资收益巨亏','最大客户占营收比过高','应收款增加,低现金流','应付债券占比高','已大比例减持','高库存隐忧','高层涉刑','营业外支出过大','负债率逐年递增','员工人数陡降','高预付款隐忧','研发费用减少','研发人员减少','被调出重要指数','员工人数偏少','毛利率偏低','高销售费用','货币资产充足仍举债','存货同比大幅增加','交易异常监管','被频繁问询监管','采购暂停或市场禁入','负面舆情','交易所警示','三费占营收比例高','控制权纠纷或争斗','股权冻结') and reason REGEXP '(?=.*2025)')
          |""".stripMargin)
      risk_df.where("`风险类型` not in ('股东大会','预约披露时间_巨潮资讯')")
        .createOrReplaceTempView("venture2")

      // 风险等级关联
      val risk_level_df = spark.sql(
        """
          |select `代码`,`风险类型`,level from venture2 left join mrl on venture2.`风险类型` = mrl.risk
          |""".stripMargin)
//      risk_level_df.where("level is null").select("`风险类型`").distinct().show(4000,false)
      risk_level_df.createOrReplaceTempView("venture3")

      //压力支撑数据rd_count_df
      val ps2_df = ps_df.where(s"trade_time='$yes_day'")
      println("========================================================================================ps2_df")
//      ps2_df.show()
      ps2_df.createOrReplaceTempView("ps")


      //TODO 分析最近涨停板（涨停板上）与当前交易日的上一交易日（yes_day）的涨跌幅差,再通过压力位支撑位确定位置，排除不在模式内的票
      val ztb_xj_df = spark.sql(
        s"""
          select `股票代码`,`股票简称`,max_date,max(ztj) as ztj,max(xj) as xj,round((max(xj)-max(ztj))/max(ztj)*100,2) as zdf from
              (select ztb2.*,if(d2.t1_trade_date=ztb2.max_date,d2.t1_close,null) as ztj,if(d2.t1_trade_date='$yes_day',d2.t1_close,null) as xj,t2_spzf from
                  (select `股票代码`,`股票简称`,max(trade_date) as max_date from ztb
                   where trade_date>='$n_day_ago' and trade_date<='$yes_day'
                   group by `股票代码`,`股票简称`) as ztb2
              left join (select * from data10_df where t1_trade_date>='$n_day_ago' and t1_trade_date<='$yes_day') as d2
              on ztb2.`股票代码` = d2.`stock_code`)
          where ztj is not null or xj is not null
          group by `股票代码`,`股票简称`,max_date
           """.stripMargin).orderBy("zdf")
      println("========================================================================================ztb_xj_df")
//      ztb_xj_df.show(10,false)
//      println(ztb_xj_df.count())
      ztb_xj_df.createOrReplaceTempView("ztb_xj")


      //where zdf<=-6.28
      val ztb_shuffer_df = spark.sql(
        """
          |select * from ztb_xj left join ps on ztb_xj.`股票代码` = ps.stock_code
          |""".stripMargin)
      println("========================================================================================ztb_shuffer_df")
//      ztb_shuffer_df.show(10,false)
//      println(ztb_shuffer_df.count())
      ztb_shuffer_df.createOrReplaceTempView("ztb_shuffer")   //涨停板洗过的票，涨跌幅低于涨停板6.28黄金分割位

      //基础数据对涨停板以及压力支撑位进行过滤
      val base_filter_ps_df = spark.sql(
        """
          |select * from basequery as b left join ztb_shuffer as z on b.`代码`=z.`股票代码`
          |where `股票代码` is not null
          |""".stripMargin)
      println("========================================================================================base_filter_ps_df")
//      base_filter_ps_df.show(10,false)
//      println(base_filter_ps_df.count())
      base_filter_ps_df.createOrReplaceTempView("bqps")

      // 关联风险表集合
      val risk2_df = spark.sql(
        s"""
           select bqps.*,total_score,r2.levels,r2.fxlxs from bqps left join
             (select q1.`代码` as dm,`简称` as jc,SUM(level) AS total_score,collect_list(level) as levels,collect_list(`风险类型`) as fxlxs,'$setdate' as trade_date,'$yes_day' as yes_day
             from basequery as q1 left join venture3 as q2 on q1.`代码`=q2.`代码`
             group by q1.`代码`,`简称`) as r2 on bqps.`代码` = r2.dm and  bqps.trade_time=r2.yes_day
             order by size(fxlxs)
           """.stripMargin)
      println("========================================================================================risk2_df")
//      risk2_df.show(1000,false)
      risk2_df.createOrReplaceTempView("r2")


      val rd_count_df = spark.sql(
        s"""
          |select `股票代码`,count(1) as rd_count from rd
          |where trade_date>='$n_day_ago' and trade_date<='$yes_day'
          |group by `股票代码` order by count(1)
          |""".stripMargin)
      println("========================================================================================rd_count_df")
//      rd_count_df.show(10,false)
      rd_count_df.createOrReplaceTempView("rcd")


      //过滤公告中的风险股票 时间限制在选股前一天和选股当天
      val notice_df = spark.read.parquet("file:///D:\\gsdata\\analysis_notices")
        .where(s"time between '$yes_day_5' and '$setdate'")
      notice_df.createOrReplaceTempView("notices")

      //公告利空数据集
      val notice_lk_df = spark.sql(
        """
          |select `股票代码`,collect_list(`风险大小`) as fxdx_lk from notices
          |where `消息类型` = '利空'
          |group by `股票代码`
          |
          |""".stripMargin)
      println("========================================================================================notice_lk_df")
//      notice_lk_df.show()
      notice_lk_df.createOrReplaceTempView("lk")

      //公告利好数据集
      val notice_lh_df = spark.sql(
        """
          |select `股票代码`,collect_list(`风险大小`) as fxdx_lh from notices
          |where `消息类型` = '利好'
          |group by `股票代码`
          |
          |""".stripMargin)
      println("========================================================================================notice_lh_df")
//      notice_lh_df.show()
      notice_lh_df.createOrReplaceTempView("lh")

      /**
       * 过滤规则
       * rd_count is not null     热度曾有
       * support_ratio<140        距离支撑偏高
       * fxdx is null             公告风险类型为利空的过滤（fxdx 风险大小）
       */
      //
      //   and support_ratio<140


      val result_df = spark.sql(
        s"""
          |select r2.*,'${setdate}' as buy_date,rcd.rd_count,lk.fxdx_lk,lh.fxdx_lh,
          |if(not array_contains(levels,5)  and (total_score<=10 or total_score is null),1,0) as sx,
          |if(fxdx_lk is null,1,0) as is_fxdx_lk,
          |if(fxdx_lh is null,1,0) as is_fxdx_lh,
          |if(support_ratio<163,1,0) as s,
          |data10_df.*,
          |ztb.`连续涨停天数`
          |from r2
          |   left join rcd on r2.`代码` = rcd.`股票代码`
          |   left join lk on r2.`代码` = lk.`股票代码`
          |   left join lh on r2.`代码` = lh.`股票代码`
          |   left join data10_df on r2.`代码` = data10_df.`stock_code` and r2.trade_time = data10_df.t0_trade_date
          |   left join ztb on r2.`代码`=ztb.`股票代码` and r2.max_date = ztb.trade_date
          |order by sx,is_fxdx_lk,s
          |""".stripMargin).drop("股票代码","股票简称")

      println("========================================================================================result_df基础数据")
//      result_df.show(1000,false)

      var rd_count =""
      //热度2020-06-30以前没有此参数
      if(setdate>"2020-06-30"){
        rd_count = "rd_count is not null and"
      }
//      println(rd_count)

      println("========================================================================================result_df筛选数据")
      //and sx=1
      val filter_result_df = result_df.where(
        s"""
          |${rd_count}
          | `连续涨停天数`<=2
          |and not array_contains(levels,5)
          |and is_fxdx_lk = 1
          |and s=1
          |""".stripMargin)

      filter_result_df.orderBy("zdf")
//        .where("support_ratio*pressure_ratio<150*90")
//        .where("`代码`='000753'")
        .show(1000,false)

      println("========================================================================================g8d_df筛选数据")

      val g8d_df = result_df
          .where(
            s"""
              |${rd_count}
              |t1_zgzf>=9.5
              |and `连续涨停天数`<=1
              |and sx=1
              |and is_fxdx_lk = 1
              |and s=1""".stripMargin)
        .select("代码","trade_time","buy_date","high_low_pressure","high_low_support","channel_position","support_ratio","pressure_ratio","sx","is_fxdx_lk","s","zdf","t0_kpzf","t0_zgzf","t0_zdzf","t0_spzf","t0_qjzf","t0_stzf","t1_sfzt","t1_cjzt","t1_kpzf","t1_zgzf","t1_zdzf","t1_spzf","t1_qjzf","t1_stzf","t2_kpzf","t2_zgzf","t2_zdzf","t2_spzf","t2_qjzf","t2_stzf")

//      g8d_df.show(30)

     //刪除並更新g8（大于8个点的数据分析）
      val g8_table = "g8_data"
      // 执行删除操作
      try {
        // 通过 Spark 执行 SQL 删除语句
        // 编写 SQL 删除语句
        val deleteQuery = s"DELETE FROM $g8_table WHERE buy_date='$setdate'"
        MysqlTools.mysqlEx(g8_table, deleteQuery)
        println("数据删除成功！")
      } catch {
        case e: Exception => println(s"数据删除失败: ${e.getMessage}")
      }

      g8d_df.where(s"buy_date='$setdate'").distinct().write.mode("append").jdbc(url, g8_table, properties)


      val endm = System.currentTimeMillis()
      println("共耗时：" + (endm - startm) / 1000 + "秒")
    }


    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }
}
