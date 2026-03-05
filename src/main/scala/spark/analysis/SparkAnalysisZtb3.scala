package spark.analysis

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import spark.ParameterSet
import spark.tools.MysqlProperties
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
 * 4、流动性
 *
 */
object SparkAnalysisZtb3 {
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

    val properties = MysqlProperties.getMysqlProperties()
    val url = properties.getProperty("url")

    val startm = System.currentTimeMillis()

//    val start_time ="2020-01-01"
//    val end_time ="2022-02-10"

    val start_time ="2026-03-05"
    val end_time ="2026-03-05"

    // 涨停板dataframe
    val ztb_df: DataFrame = spark.read.jdbc(url, "ztb_day", properties)
      .select("股票代码","股票简称","连续涨停天数","trade_date","analysis","几天几板")
    ztb_df.createOrReplaceTempView("ztb")

    // 涨停炸板dataframe
    val wencaiquery_zt_zb_df: DataFrame = spark.read.jdbc(url, s"wencaiquery_zt_zb", properties)
    wencaiquery_zt_zb_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    wencaiquery_zt_zb_df.createOrReplaceTempView("wencaiquery_zt_zb")

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
    )
    println(getYearStr(start_time,end_time))
    val data20_df = spark.read.parquet(s"file:///D:\\${ParameterSet.data_content}\\gpsj_hs_10days\\trade_date_month=20[${getYearStr(start_time,end_time)}]*")
      .select(columnsList.map(col): _*)
    data20_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    data20_df.createOrReplaceTempView("data20_df")

    //压力支撑数据[统一使用40日周期压力支撑位]
    val ps_df = spark.read.parquet(s"file:///D:\\${ParameterSet.data_content}\\pressure_support_calculator\\valid_results_pressure_advanced_dip_strategy40")
      .select("stock_code","trade_time","windowSize","pivot_pressure","pivot_support","high_low_pressure","high_low_support","channel_position","support_ratio","pressure_ratio")
    //            .where(s"trade_time between '$start_time' and '$end_time'")
    ps_df.persist(StorageLevel.MEMORY_AND_DISK_SER)

    //压力支撑数据[统一使用120日周期压力支撑位]
    val ps2_df = spark.read.parquet(s"file:///D:\\${ParameterSet.data_content}\\pressure_support_calculator\\valid_results_pressure_advanced_dip_strategy120")
      .select("stock_code","trade_time","windowSize","pivot_pressure","pivot_support","high_low_pressure","high_low_support","channel_position","support_ratio","pressure_ratio")
    //            .where(s"trade_time between '$start_time' and '$end_time'")
    val columnsToRename = Seq("windowSize", "pivot_pressure", "pivot_support", "high_low_pressure", "high_low_support", "channel_position", "support_ratio", "pressure_ratio")

    // 4. 执行重命名：对指定列添加“120”后缀，其他列保持不变
    val ps_df120 = ps2_df.select(
      ps2_df.columns.map { columnName =>
        if (columnsToRename.contains(columnName)) {
          col(columnName).alias(s"${columnName}120")
        } else {
          col(columnName)
        }
      }: _*
    )
    ps_df120.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val jyrls = spark.read.jdbc(url, "data_jyrl", properties)
      .where(s"trade_status=1 and trade_date between '$start_time' and '$end_time'")
      .orderBy(col("trade_date").desc)
      .select("trade_date").collect().map(f => f.getAs[String]("trade_date")).toList

    val analysis_area_explode_df: DataFrame = spark.read.jdbc(url, "analysis_area_explode", properties)
    analysis_area_explode_df.createOrReplaceTempView("area")
    
    for (jyrl <- jyrls) {
      val startm = System.currentTimeMillis()

      val setdate = jyrl
      val date_list = spark.read.jdbc(url, "data_jyrl", properties).orderBy(col("trade_date").desc)
        .where(s"trade_status=1 and trade_date<'$jyrl'").collect().map(f => f.getAs[String]("trade_date"))
      //yes_day 设置时间的前一个交易日
      val yes_day = date_list.toList(0)
      //y_day 设置时间的前一天
      val y_day = getPreviousDay(setdate)
      val yes_day_5 = date_list.toList(5)
      val n_day_ago = date_list.toList(20)
//      println(n_day_ago)
      val year = setdate.substring(0,4)
      var nb_year = "'"+year+"'"
//      if(setdate<year+"-03-31"){
//        nb_year = nb_year+",'"+(year.toInt - 1).toString+"'"
//      }
      println(s"==========================设置时间为${setdate},则处理的是${setdate}的股票，分析股票则用上一交易日：${yes_day}============================")

      //领域sql
      val withsql =
        s"""
           |WITH processed_data AS (
           |  SELECT
           |    `股票代码`,
           |    regexp_replace(regexp_replace(`股票代码`,'[^0-9,，]',''),'，',',') AS cleaned_codes
           |  FROM area
           |  WHERE news_date between '${yes_day}' and '${y_day}' and `利空利好`='利好' and `消息大小` in ('重大','大') and
           |  `股票代码` IS NOT NULL AND `股票代码` != ''
           |),
           |exploded_codes AS (
           |  SELECT
           |    trim(exploded_code) AS stock_code,
           |    length(trim(exploded_code)) as code_len
           |  FROM processed_data
           |  LATERAL VIEW explode(
           |    filter(split(cleaned_codes, ','),x -> trim(x) != '')
           |  ) t AS exploded_code
           |)
           |""".stripMargin
      val selectsql =
        """
          |
          |SELECT
          |  stock_code,
          |  COUNT(*) AS frequency,
          |  -- 如果需要，可以计算占比
          |  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
          |FROM exploded_codes
          |WHERE code_len = 6  -- 只统计6位数
          |  AND stock_code REGEXP '^[0-9]{6}$'  -- 确保是6位纯数字
          |  and (stock_code like '00%' or stock_code like '60%' or stock_code like '30%')
          |GROUP BY stock_code
          |ORDER BY frequency DESC, stock_code
          |""".stripMargin
      val area_sql = withsql+selectsql
//      println(ressql)

      spark.sql(area_sql).createOrReplaceTempView("area_df")
//      spark.sql(area_sql).show(2000)

      //获取需要处理的股票集合
      var basequerydf: DataFrame = spark.read.jdbc(url, s"wencaiquery_basequery_$year", properties)
        .where(s"trade_date='$setdate'")
        .select("`代码`", "`简称`")
            println("querydf-----------"+basequerydf.count())
      basequerydf.createOrReplaceTempView("basequery")
//      basequerydf.select("代码").show(3000,false)

      var venturedf: DataFrame = spark.read.jdbc(url, s"wencaiquery_venture_$year", properties)
        .where(s"trade_date='$setdate'")
            println("venturedf-----------"+venturedf.count())
      venturedf.createOrReplaceTempView("venture")

      var venture_year_df: DataFrame = spark.read.jdbc(url, s"wencaiquery_venture_year", properties)
        .where(s"year in ($nb_year)")
            println("venture_year_df-----------"+venture_year_df.count())
      venture_year_df.createOrReplaceTempView("venture_year")

      //风险数据sql+通达信风险数据
      //暂时未加入：业绩公告预警,累计经营现金流偏少,关联交易风险,高商誉风险,公司被监管警示,大股东离婚,子公司风险,公允价值收益异常,主力资金卖出,交易所监管,行政监管措施或处罚,公司被立案调查,高层协助有关机构调查,股东或子公司预重整,市盈率过高,募投项目延期,A股溢价过高,分析师评级下调,股价脚踝斩,拟大比例减持,可转债临近转股期,市净率过高,停牌次数多,十大股东总持股占比小,关联人员市场禁入,公司预重整,多元化经营风险,上市首日遭爆炒,年营收偏低,可能被*ST,会计师事务所变动,审计报告其他非标,净资产可能低于亏损,审计报告保留意见,分红不达标,定期报告存疑,*ST股票,财务造假,净资产偏低,资金占用或违规担保,股价偏低,可能终止上市,年报被问询,市值偏小,拟主动退市
      val risk_df = spark.sql(
        s"""
          |(select `代码`,`风险类型` from venture_year)
          |union
          |(select `代码`,`风险类型` from venture)
          |union
          |(select stock_code as `代码`,concat('risk_',s_type) as `风险类型` from data_risk_tdx where f_type = 'ST风险和退市' and s_type not in ('年营收偏低','会计师事务所变动','审计报告其他非标','审计报告保留意见'))
          |union
          |(select stock_code as `代码`,concat('risk_',s_type) as `风险类型` from data_risk_tdx where s_type = '财报亏损' and reason REGEXP '(?=.*亿)(?=.*$year)')
          |union
          |(select  stock_code as `代码`,concat('risk_',s_type) as `风险类型`  from data_risk_tdx where s_type in ('控股股东高质押','长期不分红' ,'业绩变脸风险'  ,'大比例解禁' ,'股权分散' ,'失信被执行' ,'事故或停产' ,'未能如期披露财报' ,'银行账户被冻结' ,'高层或股东被立案调查' ,'清仓式减持' ,'债券违约' ,'理财风险' ,'补充质押' ) )
          |union
          |(select stock_code as `代码`,concat('risk_',s_type) as `风险类型` from data_risk_tdx where s_type in ('高管人员偏少','应收账款占收入比例过高','短期负债风险','高应收款隐忧','高财务费用','扣非净利润陡降风险','非经常性损益占比过高','高负债率','经营现金流风险','最大供应商采购占比高','业绩骤减','高担保比例','高质押风险','营收陡降风险','多年扣非亏损','投资收益巨亏','最大客户占营收比过高','应收款增加,低现金流','应付债券占比高','已大比例减持','高库存隐忧','高层涉刑','营业外支出过大','负债率逐年递增','员工人数陡降','高预付款隐忧','研发费用减少','研发人员减少','被调出重要指数','员工人数偏少','毛利率偏低','高销售费用','货币资产充足仍举债','存货同比大幅增加','交易异常监管','被频繁问询监管','采购暂停或市场禁入','负面舆情','交易所警示','三费占营收比例高','控制权纠纷或争斗','股权冻结') and reason REGEXP '(?=.*$year)')
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
      val ps40_df = ps_df.where(s"trade_time='$yes_day'")
      println("========================================================================================ps40_df")
      //      ps40_df.show()
      ps40_df.createOrReplaceTempView("ps40")

      //压力支撑数据rd_count_df
      val ps120_df = ps_df120.where(s"trade_time='$yes_day'")
      println("========================================================================================ps120_df")
      //      ps_df120.show()
      ps120_df.createOrReplaceTempView("ps120")

      val ztb_xj_sql = s"""
          select `股票代码`,`股票简称`,max_date,max(ztj) as ztj,max(xj) as xj,round((max(xj)-max(ztj))/max(ztj)*100,2) as zdf from
              (select ztb2.*,if(d2.t1_trade_date=ztb2.max_date,d2.t1_close,null) as ztj,if(d2.t1_trade_date='$yes_day',d2.t1_close,null) as xj,t2_spzf from
                  (select `股票代码`,`股票简称`,max(trade_date) as max_date from ztb
                   where trade_date>='$n_day_ago' and trade_date<='$yes_day'
                   group by `股票代码`,`股票简称`) as ztb2
              left join (select * from data20_df where t1_trade_date>='$n_day_ago' and t1_trade_date<='$yes_day') as d2
              on ztb2.`股票代码` = d2.`stock_code`)
          where ztj is not null or xj is not null
          group by `股票代码`,`股票简称`,max_date
           """.stripMargin
//      println(ztb_xj_sql)
//      spark.sql(s"select * from data20_df where t1_trade_date>='$n_day_ago' and t1_trade_date<='$yes_day'")
//        .orderBy("t1_trade_date").show(1000)
      val ztb_xj_df = spark.sql(ztb_xj_sql).orderBy("zdf")
      println("========================================================================================ztb_xj_df")
      //      println(ztb_xj_df.count())
      ztb_xj_df.createOrReplaceTempView("ztb_xj")

      val ztb_shuffer_df = spark.sql(
        """
          |select ztb_xj.*,ps40.*,
          |ps120.windowSize120,ps120.pivot_pressure120,ps120.pivot_support120,
          |ps120.high_low_pressure120,ps120.high_low_support120,
          |ps120.channel_position120,ps120.support_ratio120,ps120.pressure_ratio120
          |      from ztb_xj
          | left join ps40 on ztb_xj.`股票代码` = ps40.stock_code
          | left join ps120 on ztb_xj.`股票代码` = ps120.stock_code
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
//      base_filter_ps_df.show(1000,false)
      //      println(base_filter_ps_df.count())
      base_filter_ps_df.createOrReplaceTempView("bqps")

      // 关联风险表集合
      val risk2_df = spark.sql(
        s"""
           select bqps.*,total_score,r2.levels_array,r2.fxlxs,concat_ws(',',levels_array) as levels from bqps
                left join
                 (select q1.`代码` as dm,`简称` as jc,SUM(level) AS total_score,collect_list(level) as levels_array,
                 concat_ws(',',collect_list(`风险类型`)) as fxlxs,'$setdate' as trade_date,'$yes_day' as yes_day
                 from basequery as q1 left join venture3 as q2 on q1.`代码`=q2.`代码`
                 group by q1.`代码`,`简称`) as r2
             on bqps.`代码` = r2.dm and bqps.trade_time=r2.yes_day
           """.stripMargin)
      println("========================================================================================risk2_df")
//      risk2_df.show(20,false)
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
      val notice_df = spark.read.parquet(s"file:///D:\\${ParameterSet.data_content}\\analysis_notices")
        .where(s"time between '$yes_day_5' and '$setdate'")
      notice_df.createOrReplaceTempView("notices")

      //公告利空数据集
      val notice_lk_df = spark.sql(
        """
          |select `股票代码`,collect_list(`风险大小`) as fxdx_lk_array,concat_ws(',',collect_list(`风险大小`)) as fxdx_lk from notices
          |where `消息类型` = '利空' and `风险大小` is not null
          |group by `股票代码`
          |""".stripMargin)
      println("========================================================================================notice_lk_df")
      notice_lk_df.createOrReplaceTempView("lk")

      //公告利好数据集
      val notice_lh_df = spark.sql(
        """
          |select `股票代码`,collect_list(`风险大小`) as fxdx_lh_array,concat_ws(',',collect_list(`风险大小`)) as fxdx_lh from notices
          |where `消息类型` = '利好'
          |group by `股票代码`
          |
          |""".stripMargin)
      println("========================================================================================notice_lh_df")
      notice_lh_df.createOrReplaceTempView("lh")

      /**
       * 过滤规则
       * rd_count is not null     热度曾有
       * support_ratio<140        距离支撑偏高
       * fxdx_lh_array is null             公告风险类型为利空的过滤（fxdx 风险大小）
       *
       * if(not array_contains(levels,5)  and (total_score<=10 or total_score is null),1,0) as sx,
       * if(fxdx_lk_array is null,1,0) as is_fxdx_lk,
       * if(fxdx_lh_array is not null,1,0) as is_fxdx_lh,
       * if(support_ratio<163,1,0) as s,
       *
       */

      val mid_df = spark.sql(
        s"""
           |select r2.*,'${setdate}' as buy_date,rcd.rd_count,lk.fxdx_lk,lh.fxdx_lh,
           |data20_df.*,
           |ztb.`连续涨停天数`
           |from r2
           |   left join rcd on r2.`代码` = rcd.`股票代码`
           |   left join lk on r2.`代码` = lk.`股票代码`
           |   left join lh on r2.`代码` = lh.`股票代码`
           |   left join data20_df on r2.`代码` = data20_df.`stock_code` and r2.trade_time = data20_df.t0_trade_date
           |   left join ztb on r2.`代码`=ztb.`股票代码` and r2.max_date = ztb.trade_date
           |order by buy_date
           |""".stripMargin).drop("股票代码")

      println("========================================================================================mid_df基础数据")
//      mid_df.show(1000,false)
      mid_df.createOrReplaceTempView("mid")

      val result_df = spark.sql(
        s"""
          |select t1.*,ztzb.`首次涨停时间`,frequency,
          |   round((1+(t1_spzf-t1_zgzf+(t2_kpzf+t2_zgzf+t2_spzf)/3)/100-0.0017),2) as result,
          |   round(if(t2_zgzf<=0,1+(t1_spzf-t1_zgzf+t2_spzf)/100-0.0017,1+(t1_spzf-t1_zgzf+(t2_kpzf+t2_zgzf+t2_spzf)/3)/100-0.0017),2) as result2,
          |   round(if(t2_zgzf<=0,1+(t1_spzf-t1_zgzf+t2_spzf)/100-0.0017,1+(t1_spzf-t1_zgzf+(t2_kpzf+t2_zgzf+t2_zdzf+t2_spzf)/4)/100-0.0017),2) as result3
          |from mid as t1
          |   left join wencaiquery_zt_zb as ztzb on t1.`代码`=ztzb.`股票代码` and t1.buy_date=ztzb.trade_date
          |   left join area_df  on t1.`代码`=area_df.`stock_code`
          |""".stripMargin)

      println("========================================================================================result_df")
//      result_df.show(20)

      println("========================================================================================filter_result_df")

      var rd_count =""
      //热度2020-06-30以前没有此参数
      if(setdate>"2020-06-30"){
        rd_count = "and rd_count is not null"
      }

      //and frequency is not null
      val filter_result_df = result_df.where(s"""
          |buy_date like '%${jyrl}%'
          |${rd_count}
          |and t0_zgzf<3
          |and t0_stzf<3
          |and support_ratio*pressure_ratio<14000

          |and (fxdx_lk not like '%重大%' or fxdx_lk is null)
          |and (total_score<=15 or total_score is null)
          |and levels not like '%5%'

          |""".stripMargin)
        .orderBy("zdf")

      filter_result_df.show(1000)

//      filter_result_df.orderBy("zdf")
//        .show(1000)

      val g8d_df = result_df
        .where("t1_sfzt=1 or t1_cjzt=1")
        .select("代码","`股票简称`",
          "max_date","trade_time","buy_date",
          "high_low_pressure","high_low_support","channel_position","support_ratio","pressure_ratio",
          "zdf",
          "t0_stzf",
          "t1_sfzt","t1_cjzt","t1_kpzf","t1_zgzf","t1_zdzf","t1_spzf","t1_qjzf","t1_stzf",
          "t2_kpzf","t2_zgzf","t2_zdzf","t2_spzf","t2_qjzf","t2_stzf",
          "`连续涨停天数`","`首次涨停时间`","levels","total_score","fxdx_lk","fxdx_lh","rd_count","result","result2","result3")

//      g8d_df.show(1000)

      //刪除並更新g8（大于8个点的数据分析）
      val g8_table = "g8_data_ztb3"
      // 执行删除操作
      try {

        // 通过 Spark 执行 SQL 删除语句
        // 编写 SQL 删除语句
        val deleteQuery = s"DELETE FROM $g8_table WHERE buy_date='$setdate'"
//        println(deleteQuery)
        MysqlTools.mysqlEx(deleteQuery)
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


  def getYearStr(start_time:String,end_time:String):String={
    // 提取年份的后两位
    val start_year_short = start_time.substring(2, 4).toInt  // 19
    val end_year_short = end_time.substring(2, 4).toInt      // 25

    // 生成年份序列并拼接成字符串
    val result = (start_year_short to end_year_short)
      .map(_.toString)
      .mkString(",")

    result
  }

  def getPreviousDay(dateStr: String): String = {
    // 定义日期格式
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    // 解析字符串为LocalDate，减去一天，再格式化为字符串
    val date = LocalDate.parse(dateStr, formatter)
    val previousDate = date.minusDays(1)

    previousDate.format(formatter)
  }
}

