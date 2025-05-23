package sparktask.adata.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 参考指标
 */
object ReferenceIndicators {
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
    val startm = System.currentTimeMillis()

    val df2 = spark.read.parquet("file:///D:\\gsdata\\gpsj_day_all_hs\\trade_date_month=2024-1*")
    df2.cache()
    df2.createTempView("ta1")

//    spark.sql(
//      """select trade_date,
//        |sum(if(kxzt='红柱',1,0)) as `红柱`,
//        |sum(if(kxzt='红柱上影线',1,0)) as `红柱上影线`,
//        |sum(if(kxzt='红柱下影线',1,0)) as `红柱下影线`,
//        |sum(if(kxzt='红柱十字星',1,0)) as `红柱十字星`,
//        |sum(if(kxzt='绿柱',1,0)) as `绿柱`,
//        |sum(if(kxzt='绿柱上影线',1,0)) as `绿柱上影线`,
//        |sum(if(kxzt='绿柱下影线',1,0)) as `绿柱下影线`,
//        |sum(if(kxzt='绿柱十字星',1,0)) as `绿柱十字星`,
//        |sum(if(kxzt='涨停一字',1,0)) as `涨停一字`,
//        |sum(if(kxzt='跌停一字',1,0)) as `跌停一字`,
//        |sum(if(kxzt='其他',1,0)) as `其他`,
//        |round(sum(if(kxzt in ('红柱','红柱上影线','红柱下影线','红柱十字星','涨停一字'),1,0))/count(1)*100,2) as `红股比率`,
//        |round(sum(if(kxzt in ('绿柱','绿柱上影线','绿柱下影线','绿柱十字星','跌停一字'),1,0))/count(1)*100,2) as `绿股比率`,
//        |sum(if(sfzt='1',1,0)) as `涨停数`,
//        |sum(if(cjzt='1',1,0)) as `炸板数`,
//        |sum(if(sfdt='1',1,0)) as `跌停数`,
//        |sum(if(cjdt='1',1,0)) as `翘板数`,
//        |round(sum(if(sfzt='1',1,0))/(sum(if(sfzt='1',1,0))+sum(if(cjzt='1',1,0)))*100,2) as `封板率`,
//        |round(sum(if(cjzt='1',1,0))/(sum(if(sfzt='1',1,0))+sum(if(cjzt='1',1,0)))*100,2) as `炸板率`
//        | from ta1 group by trade_date order by trade_date desc""".stripMargin).show(200)

    spark.sql("""select *,spzf,ROW_NUMBER() OVER (partition by stock_code ORDER BY stock_code,trade_date) AS row from ta1 order by stock_code,trade_date desc""")
      .where("stock_code='000004' ").show(200)
    /**
     * 计算当天后N天的收盘和，sp2为两天的收盘和，以此类推
     * 前N天最小值最大值确定振幅，最好平稳，可以分组统计区间确定
     * 前期连板数大于4的不做，这样的票有上涨的概率大
     * 以涨停为标的（首板），涨停价为标准
     * 确定锚定日期的涨跌幅
     */

    //SUM(CASE WHEN sfzt = 1 THEN 1 ELSE 0 END) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS UNBOUNDED PRECEDING) -
    //SUM(CASE WHEN sfzt = 0 THEN 1 ELSE 0 END) OVER (PARTITION BY stock_code ORDER BY trade_date ROWS UNBOUNDED PRECEDING) AS `连板天数`,
    spark.sql(
      """
        |select
        |stock_code,trade_date,sfzt,cjzt,close,
        |min(low) over(partition by stock_code ORDER BY stock_code,row ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as `前20日最小值`,
        |max(high) over(partition by stock_code ORDER BY stock_code,row ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as `前20日最大值`,
        |sum(sfzt) over(partition by stock_code ORDER BY stock_code,row ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) as `前两日是否涨停`,
        |sum(sfzt) over(partition by stock_code ORDER BY stock_code,row ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) as `后两日是否涨停`,
        |sum(spzf) over(partition by stock_code ORDER BY stock_code,row ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) as sp2,
        |sum(spzf) over(partition by stock_code ORDER BY stock_code,row ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) as sp3,
        |sum(spzf) over(partition by stock_code ORDER BY stock_code,row ROWS BETWEEN 1 FOLLOWING AND 4 FOLLOWING) as sp4,
        |sum(spzf) over(partition by stock_code ORDER BY stock_code,row ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING) as sp5,
        |sum(spzf) over(partition by stock_code ORDER BY stock_code,row ROWS BETWEEN 1 FOLLOWING AND 6 FOLLOWING) as sp6,
        |sum(spzf) over(partition by stock_code ORDER BY stock_code,row ROWS BETWEEN 1 FOLLOWING AND 7 FOLLOWING) as sp7,
        |sum(spzf) over(partition by stock_code ORDER BY stock_code,row ROWS BETWEEN 1 FOLLOWING AND 8 FOLLOWING) as sp8,
        |sum(spzf) over(partition by stock_code ORDER BY stock_code,row ROWS BETWEEN 1 FOLLOWING AND 9 FOLLOWING) as sp9,
        |sum(spzf) over(partition by stock_code ORDER BY stock_code,row ROWS BETWEEN 1 FOLLOWING AND 10 FOLLOWING) as sp10
        |from
        |(select *,spzf,ROW_NUMBER() OVER (partition by stock_code ORDER BY stock_code,trade_date) AS row from ta1 order by stock_code,trade_date desc)
        |""".stripMargin)
      .where("(sfzt=1 or cjzt=1) and trade_date='2024-12-02' and `前两日是否涨停`=0 and `后两日是否涨停`=0 ")
      .show(2000)



    val endm = System.currentTimeMillis()
    println("共耗时："+(endm-startm)/1000+"秒")
    spark.close()
  }
}
