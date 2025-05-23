package sparktask.Iwencai.statistics

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import sparktask.tools.MysqlTools

/**
 * 涨停板的分布
 */
object risk_ztb {
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

    val pattern = "(?:商标无效|主营业务萎缩|重大诉讼未决|亏损|配股失败|反倾销|债务逾期|裁员,大幅裁员|审计非标意见|泄露|审计非标|减持预披露|虚增收入|项目叫停|换手率过高|换手率畸高|信息披露违规|被证监会立案调查|公章失控|和解协议失效|诉讼未决|未履行程序|非理性交易|股东大会无效|业绩下修|大股东减持计划|移出指数成分股|调解失败|风险提示|订单取消|公安立案|质押延期,债务重组|IPO撤回|业绩变脸|股权冻结,质押爆仓|产品下架|终止|配资风险|补充质押|业绩预减|业绩预告修正|GMP证书撤回|减持实施|敏感期交易|业绩快报修正|客户流失,重大客户流失|员工持股计划减持|生态破坏|暂停上市|内幕信息|未达业绩承诺|加速到期|消防整改|一致行动人变动|业务收缩|实际控制人变更|质押违约|利润分配取消|长臂管辖|反诉|计提大额资产减值|通报批评|价格战|恢复上市失败|稽查立案|违约金|集体诉讼|净利润亏损|市净率高于行业|重大事故|欠税公告|盈利水平有限|审计保留意见,非标审计|警示函|税务处罚|海外资产冻结,异常波动|违规担保|合作方撤资|召回|被列入实体清单|严重异常波动|对外投资亏损|存货减值|政策调整影响（如补贴退坡|股市有风险|项目终止|被执行|黑客攻击|重组失\\\\ 败,收购终止|项目烂尾|资质丢失|下滑|资产查封|劳动纠纷|持续经营能力存疑|减持计划未实施|做空报告|更正公告|限售解禁|纪律处分|技术授权终止|炒作风险|并购标的业绩不达预期|没收违法所得|负面舆情|重组失败|内幕交易,游资炒作|拍卖流拍|交易异常波动|资产流拍|停业|行政监管措施|安全事故|分红取消|质量事故|利益输送|营收下降|技术泄密|业绩补偿|由盈转亏|修正业绩预告（向下修正）|st|市场份额\\\\ 下降|偿债压力加大|质押比例|医保目录移出,\\\\ 停产|被动减持|取消转增,补缴税款|发债失败|市盈率偏离|侵权|评级下调|减持|参股公司暴雷|终审败诉,国际仲裁|政府接管|监管措施|信用评级下调|产能闲置|供应商断供|股价异动|重大工程质量问题|协议转让|连续亏损|市场情绪过热|重大罚单|退市风险,退市整理期|游行|内控缺陷|数据泄露|毛利率下降,净利润\\\\ 下滑|收回|研发进度延迟|技术封锁|产品滞销|内部控制缺陷|应收账款逾期|关联交易占比过高|司法冻结|造假|诉讼|居民投诉|列入观察名单|严重异动|会计调整|融资渠\\\\ 道受限|限高令|资本充足率不足|盈利预测下调|质押展期|坏账准备增加|签字会计师被罚,保荐机构被罚|审计机构被查|短期涨幅较大|订单萎缩|减持计划|强制平仓,环保处罚|罢工|竞争加剧|净利润同比下降|合同纠纷|生产批件吊销|税收优惠取消|信用减值,大额计提|贸易制裁|行业政策调整|重整计划未通过|矿权灭失,诉讼|司法拍卖|客户集中度高|产能利用率不足|政策风险|药监飞检|扣非后净利为负|罚款|注册申请终止|撤销|股吧传闻|被执行人|退市整理期|立案调查|证监会调查|资金链断裂|和解协议|现金流紧张|财务费用高企|赔偿|失败|施工事故|质押风险|概念炒作|ST|安监处罚|重大诉讼/仲裁|反补贴|约谈|股东减持|坏账计提|销售腰斩|风险警示|自然灾害影响|重大违法退市|收到证监会立案调查|工程延期|库存积压|限制高消费|行政处罚|337调查|被诉|新业务未形成收入|董事会决议撤销|补贴收回|限售股解禁|质押回购,限售解禁|香橼做空|仲裁|延迟披露|项目停滞|减持完成|破产申请,清算组进驻|可转债中止|未履行程\\\\ 序|质押解除|证券虚假陈述,投资者索赔|产品召回|非理性炒作|持股5%以上股东拟减持|理财产品\\\\ 违约|许可证到期|监事质疑|索赔金额巨大|核心人员离职,研发失败|下调盈利指引|火灾|业绩下滑|市盈率\\\\(TTM\\\\)异常|独董异议|破产清算|担保代偿风险|药品召回|食品安全事故|跨境追责|退市|媒体报道不实|赔偿金|研发失败|不良率飙升|营收大幅减少|集采丢标|罚单|特许经营权收回|债务和解|责令改正,警示函|窗口期交易|年报难产|质押比例过高|业绩对赌,业绩变脸|收入确认存疑|退市风险警示|巨亏|股权转让未获批|误导性陈述,重大遗漏|操纵市场,短线交易|债券违约|补助终止,融资失败|股价操纵|协议终止|短线交易|下架|预售证吊销|问询函|董事反对|小道消息|股权纷争|可能对股价造成负面影响|部门撤销|失信被执行人|战略调整|海外诉讼|核心客户流失|减亏|非法证券活动|发债中止|暴雷|对赌失败|债\\\\ 转股|预亏|\\\\ 破产重整|土地闲置|项目延期|大规模关店|未及时披露|专利权失效|一致行动人变更|被动平仓风险|群体事件|资产冻结|起诉|计提大额坏\\\\ 账|财产保全|投资未达预期|失效|司法划转|退市预警|价格倒挂,毛利率下滑|银行抽贷|质押|定增终止|原材料涨价|澄清公告,股票质押|解散|资金占用|债务|丢失资质|营收扣除后不足|财报更正|订单不足|监管函|专利纠纷|停牌核查|终身禁入|营收不达标|行政处罚,市场禁入|出售资产受阻|虚假记载|资不抵债|评级展望负面|净资产为负|控股股东质押|债权人起诉,信用评级下调|生产线停产|延期|挤兑|市值退市|失信|经销商造反|毛利率下降|临床试验失败|技术落后|关联方利益输送|禁售|重大传闻|核心设备故障|增持未实施|诉讼仲裁|以物抵债,违约金|合同解除|原材料短缺|飞行检查不合格,临床试验暂停|在建工程转固压力|爆炸|停工|暂停分红|扣非后净利润为负|禁运|制裁升级|客户流失|预重整失败,立案调查|高管被调查,董秘被约谈|重大安全事故|强制执行|出口管制,实体\\\\ 清单|营业执照吊销,经营异常名录|连续涨停|交易所问询|暴利诱惑|融资成本上升|浑水做空|中标金额存不确定性|业绩不达预期|违规减持|借钱投资|门店关闭|暂不受理|债务展期|研发终止|账户冻结,股权冻结|停产|交叉违约|批文过期|散户维权|责令改正|面值退市|实控人失联|债务违约|事故|核\\\\ 心资产查封|卖房炒股|成交量异常,换手率异常|股权质押平仓风险|土地收回|订单不及预期|批件收回|高管集中抛售|退市风险|关联交易违规,资金占用|预计年度亏损|信披违规|营收低于3亿元|核心技术人员离职|公司治理失效,内部控制否定意见|重大合同终止|限产限电）|环境污染,环保督察|行业竞争加剧|银行授信额度缩减|股份冻结|商标侵权|买者自负|净资产为\\\\ 负|累计涨幅偏离|会计差错|毛利率下滑|股权司法划转|原材料价格暴涨|审计意见非标|违约|债务违约风险|补充披露|专利侵权|董事会成员变动频繁|重大资产重组终止|战略投资者退出,项目烂尾|业绩大幅下滑|公开谴责|媒体曝光,品牌危机|业绩承诺未达标|关注函|供应链中断|律所被立案|暂停营业|\\\\*ST|续亏|表决权委托到期|成本失控|收购终止|技术迭代风险|减持届满|上诉|子公司破产清算|工厂关闭|库存积压,存货跌价|利息违约,资不抵债|许可证吊销,经营资质到期|赔\\\\ 偿|异常波动|私有化失败|专利无效|核心专利到期|董事长/CEO辞职|核心技术人员流失|重整投资人退出|政府补助不可持续|内幕交易|财务类退市|政府补助减少|暂停|GMP撤销|重大违法强制退市|存在重大不确定性|增持承诺未履行|波动|控制权争夺|配股中止|质押平仓|商誉减值|认证失效|资产减值|终止上市|环保处罚|资产剥离失败|营业收入扣除后不达标|贷款逾期|再审|财务造假|成本上升|清仓减持,大宗减持)"

    // 定义正则提取所有匹配项的UDF
    val extractAllMatches = udf((text: String, regexPattern: String) => {
      if (text == null) Array.empty[String]
      else {
        val pattern = regexPattern.r
        pattern.findAllIn(text).toArray
      }
    })

    // 注册UDF
    spark.udf.register("extract_all_matches", extractAllMatches)

    val ztbdf: DataFrame = spark.read.jdbc(url, s"ztb_day", properties)
      .where("`股票代码` rlike '^(00|60)' and `股票简称` not like '%st%' and `股票简称` not like '%ST%' and `连续涨停天数`=1")
      .distinct()

    ztbdf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    ztbdf.createOrReplaceTempView("ztb_day")

    val jyrldf: DataFrame = spark.read.jdbc(url, "data_jyrl", properties)
    jyrldf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    jyrldf.createOrReplaceTempView("data_jyrl")

    val listsetDate = spark.sql("select distinct year(trade_date) as year from data_jyrl where  trade_date between '2015-04-22' and '2025-05-13' and trade_status='1'")
      .collect()
      .map(f => f.getAs[Int]("year")).toList
    listsetDate.foreach(println)
    for(year<-listsetDate) {
      var querydf: DataFrame = spark.read.jdbc(url, s"wencaiquery_venture_$year", properties)
      querydf.createOrReplaceTempView("venture")

      var jhsaggg_df: DataFrame = spark.read.jdbc(url, s"jhsaggg$year", properties)
      jhsaggg_df.createOrReplaceTempView("jhsaggg")

//
      val hit_risk = spark.sql(
        s"""
           |select `代码`,`公告日期`,`公告标题`,'公告风险' as `风险类型`,extract_all_matches(`公告标题`, '$pattern') AS hit_words from jhsaggg
           |where `公告标题` rlike '$pattern'
           |order by `公告日期` desc
           |""".stripMargin)
//      hit_risk.show(false)
      hit_risk.createOrReplaceTempView("hit")

      spark.sql(
        """
          |select z.*,yes_day from ztb_day as z left join (select *,lag(trade_date,1) over(order by trade_date) as yes_day from data_jyrl where trade_status=1) as d on z.trade_date=d.trade_date
          |""".stripMargin).createOrReplaceTempView("ztb_day2")

      val gl_df = spark.sql(
        """
          |select z.trade_date,`股票代码` as stock_code,`股票简称`,`风险类型`,hit_words  from ztb_day2 as z left join hit as h on z.`股票代码`=h.`代码` and z.trade_date>h.`公告日期` and z.yes_day<=h.`公告日期`
          |where `代码` is not null
          |order by trade_date desc,`股票代码` desc
          |""".stripMargin)
//      gl_df.show(false)
      gl_df.createOrReplaceTempView("gl")

      val gl2_df = spark.sql(
        """
          |select trade_date,stock_code,`股票简称`,`风险类型`,concat_ws(',',collect_list(hit_word)) as hit_words from
          | (select trade_date,stock_code,`股票简称`,`风险类型`,hit_word from gl lateral view explode(hit_words) as hit_word)
          |group by trade_date,stock_code,`股票简称`,`风险类型`
          |""".stripMargin)
//      gl2_df.show(false)
      gl2_df.createOrReplaceTempView("gl2")

      val gl3_df = spark.sql(
        s"""
          |select ztb_day2.trade_date,`代码` as stock_code,`简称` ,`风险类型`,'' as hit_words from ztb_day2 left join venture
          |on ztb_day2.`股票代码`=venture.`代码` and ztb_day2.trade_date=venture.trade_date
          |where `代码` is not null and `风险类型`!='公告风险'
          |""".stripMargin)
//      gl3_df.show(100)
      gl3_df.createOrReplaceTempView("gl3")

      val df2 = spark.sql("""(select * from gl2) union (select * from gl3)""")
//        df2.show(10000)

      //删除日期内的条件数据并重写
      try {
        // 通过 Spark 执行 SQL 删除语句
        val deleteQuery = s"DELETE FROM ztb_risk_filter WHERE trade_date like '$year-%'"
        MysqlTools.mysqlEx("ztb_risk_filter", deleteQuery)
        println("数据删除成功！")
      } catch {
        case e: Exception => println(s"数据删除失败: ${e.getMessage}")
      }

      df2.write.mode("append").jdbc(url, "ztb_risk_filter", properties)

    }


    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }
}
