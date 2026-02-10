package spark.bus.stock

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.ParameterSet
import sparktask.tools.FileTools

/**
 * 总线3
 * 制作宽表,增量宽表
 */
object Bus_3_SparkMakeWideTableIncrement {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions","100")
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

    //制作增量宽表的路径gsdata
    val months = "2025-10,2025-11,2025-12"
    makeWide(spark,months)

    val endm = System.currentTimeMillis()
    println("共耗时："+(endm-startm)/1000+"秒")
    spark.close()
  }

  def makeWide(spark:SparkSession,months:String): Unit ={
    val m1 = months.split(",")(0)
    val m2 = months.split(",")(1)
    val m3 = months.split(",")(2)

    val df2 = spark.read.parquet(s"file:///D:\\${ParameterSet.data_content}\\gpsj_day_all_hs\\trade_date_month=$m1",
      s"file:///D:\\${ParameterSet.data_content}\\gpsj_day_all_hs\\trade_date_month=$m2",
      s"file:///D:\\${ParameterSet.data_content}\\gpsj_day_all_hs\\trade_date_month=$m3")

    df2.createTempView("ta2")

    val df3 = spark.sql("select *,row_number() over(partition by stock_code order by trade_date,stock_code) as row from ta2 ")
    df3.createTempView("ta3")

    val df4 = spark.sql(
      """
        |select t2.*,
        |if(t2.high<t1.high,'下探',if(t2.high=t1.high,'平整','突破')) as sx,
        |if(t2.low<t1.low,'下探',if(t2.low=t1.low,'平整','突破')) as xx,
        |if(t2.volume<t1.volume,'缩量',if(t2.volume=t1.volume,'平量',if(t2.volume/t1.volume>2,'放巨量','放量'))) as ln,
        |round(t2.volume/t1.volume,2) as zrlnb
        |from ta3 as t1 left join ta3 as t2 on t1.row+1=t2.row and t1.stock_code=t2.stock_code
        |where t2.row is not null """.stripMargin)

    df4.createTempView("ta4")

    val df5 = spark.sql(
      """|select t0.stock_code,substring(t0.trade_date,0,7) as trade_date_month,
         |t0.trade_date as t0_trade_date,t0.bk as t0_bk,t0.sfzt as t0_sfzt,t0.cjzt as t0_cjzt, t0.open as t0_open , t0.close as t0_close ,t0.high as t0_high,t0.low as t0_low,t0.kpzf as t0_kpzf,t0.spzf as t0_spzf,t0.zgzf as t0_zgzf,t0.zdzf as t0_zdzf,t0.volume as t0_volume,t0.amount as t0_amount,t0.pre_close as t0_pre_close,t0.qjzf as t0_qjzf,t0.stzf as t0_stzf,t0.kxzt as t0_kxzt,t0.kp as t0_kp,t0.wp as t0_wp ,t0.sx as t0_sx,t0.xx as t0_xx,t0.ln as t0_ln,t0.zrlnb as t0_zrlnb,t0.turnover_ratio as t0_turnover_ratio,t0.change as t0_change,t0.change_pct as t0_change_pct,
         |t1.trade_date as t1_trade_date,t1.bk as t1_bk,t1.sfzt as t1_sfzt,t1.cjzt as t1_cjzt, t1.open as t1_open , t1.close as t1_close ,t1.high as t1_high,t1.low as t1_low,t1.kpzf as t1_kpzf,t1.spzf as t1_spzf,t1.zgzf as t1_zgzf,t1.zdzf as t1_zdzf,t1.volume as t1_volume,t1.amount as t1_amount,t1.pre_close as t1_pre_close,t1.qjzf as t1_qjzf,t1.stzf as t1_stzf,t1.kxzt as t1_kxzt,t1.kp as t1_kp,t1.wp as t1_wp ,t1.sx as t1_sx,t1.xx as t1_xx,t1.ln as t1_ln,t1.zrlnb as t1_zrlnb,t1.turnover_ratio as t1_turnover_ratio,t1.change as t1_change,t1.change_pct as t1_change_pct,
         |t2.trade_date as t2_trade_date,t2.bk as t2_bk,t2.sfzt as t2_sfzt,t2.cjzt as t2_cjzt, t2.open as t2_open , t2.close as t2_close ,t2.high as t2_high,t2.low as t2_low,t2.kpzf as t2_kpzf,t2.spzf as t2_spzf,t2.zgzf as t2_zgzf,t2.zdzf as t2_zdzf,t2.volume as t2_volume,t2.amount as t2_amount,t2.pre_close as t2_pre_close,t2.qjzf as t2_qjzf,t2.stzf as t2_stzf,t2.kxzt as t2_kxzt,t2.kp as t2_kp,t2.wp as t2_wp ,t2.sx as t2_sx,t2.xx as t2_xx,t2.ln as t2_ln,t2.zrlnb as t2_zrlnb,t2.turnover_ratio as t2_turnover_ratio,t2.change as t2_change,t2.change_pct as t2_change_pct,
         |t3.trade_date as t3_trade_date,t3.bk as t3_bk,t3.sfzt as t3_sfzt,t3.cjzt as t3_cjzt, t3.open as t3_open , t3.close as t3_close ,t3.high as t3_high,t3.low as t3_low,t3.kpzf as t3_kpzf,t3.spzf as t3_spzf,t3.zgzf as t3_zgzf,t3.zdzf as t3_zdzf,t3.volume as t3_volume,t3.amount as t3_amount,t3.pre_close as t3_pre_close,t3.qjzf as t3_qjzf,t3.stzf as t3_stzf,t3.kxzt as t3_kxzt,t3.kp as t3_kp,t3.wp as t3_wp ,t3.sx as t3_sx,t3.xx as t3_xx,t3.ln as t3_ln,t3.zrlnb as t3_zrlnb,t3.turnover_ratio as t3_turnover_ratio,t3.change as t3_change,t3.change_pct as t3_change_pct,
         |t4.trade_date as t4_trade_date,t4.bk as t4_bk,t4.sfzt as t4_sfzt,t4.cjzt as t4_cjzt, t4.open as t4_open , t4.close as t4_close ,t4.high as t4_high,t4.low as t4_low,t4.kpzf as t4_kpzf,t4.spzf as t4_spzf,t4.zgzf as t4_zgzf,t4.zdzf as t4_zdzf,t4.volume as t4_volume,t4.amount as t4_amount,t4.pre_close as t4_pre_close,t4.qjzf as t4_qjzf,t4.stzf as t4_stzf,t4.kxzt as t4_kxzt,t4.kp as t4_kp,t4.wp as t4_wp ,t4.sx as t4_sx,t4.xx as t4_xx,t4.ln as t4_ln,t4.zrlnb as t4_zrlnb,t4.turnover_ratio as t4_turnover_ratio,t4.change as t4_change,t4.change_pct as t4_change_pct,
         |t5.trade_date as t5_trade_date,t5.bk as t5_bk,t5.sfzt as t5_sfzt,t5.cjzt as t5_cjzt, t5.open as t5_open , t5.close as t5_close ,t5.high as t5_high,t5.low as t5_low,t5.kpzf as t5_kpzf,t5.spzf as t5_spzf,t5.zgzf as t5_zgzf,t5.zdzf as t5_zdzf,t5.volume as t5_volume,t5.amount as t5_amount,t5.pre_close as t5_pre_close,t5.qjzf as t5_qjzf,t5.stzf as t5_stzf,t5.kxzt as t5_kxzt,t5.kp as t5_kp,t5.wp as t5_wp ,t5.sx as t5_sx,t5.xx as t5_xx,t5.ln as t5_ln,t5.zrlnb as t5_zrlnb,t5.turnover_ratio as t5_turnover_ratio,t5.change as t5_change,t5.change_pct as t5_change_pct,
         |t6.trade_date as t6_trade_date,t6.bk as t6_bk,t6.sfzt as t6_sfzt,t6.cjzt as t6_cjzt, t6.open as t6_open , t6.close as t6_close ,t6.high as t6_high,t6.low as t6_low,t6.kpzf as t6_kpzf,t6.spzf as t6_spzf,t6.zgzf as t6_zgzf,t6.zdzf as t6_zdzf,t6.volume as t6_volume,t6.amount as t6_amount,t6.pre_close as t6_pre_close,t6.qjzf as t6_qjzf,t6.stzf as t6_stzf,t6.kxzt as t6_kxzt,t6.kp as t6_kp,t6.wp as t6_wp ,t6.sx as t6_sx,t6.xx as t6_xx,t6.ln as t6_ln,t6.zrlnb as t6_zrlnb,t6.turnover_ratio as t6_turnover_ratio,t6.change as t6_change,t6.change_pct as t6_change_pct,
         |t7.trade_date as t7_trade_date,t7.bk as t7_bk,t7.sfzt as t7_sfzt,t7.cjzt as t7_cjzt, t7.open as t7_open , t7.close as t7_close ,t7.high as t7_high,t7.low as t7_low,t7.kpzf as t7_kpzf,t7.spzf as t7_spzf,t7.zgzf as t7_zgzf,t7.zdzf as t7_zdzf,t7.volume as t7_volume,t7.amount as t7_amount,t7.pre_close as t7_pre_close,t7.qjzf as t7_qjzf,t7.stzf as t7_stzf,t7.kxzt as t7_kxzt,t7.kp as t7_kp,t7.wp as t7_wp ,t7.sx as t7_sx,t7.xx as t7_xx,t7.ln as t7_ln,t7.zrlnb as t7_zrlnb,t7.turnover_ratio as t7_turnover_ratio,t7.change as t7_change,t7.change_pct as t7_change_pct,
         |t8.trade_date as t8_trade_date,t8.bk as t8_bk,t8.sfzt as t8_sfzt,t8.cjzt as t8_cjzt, t8.open as t8_open , t8.close as t8_close ,t8.high as t8_high,t8.low as t8_low,t8.kpzf as t8_kpzf,t8.spzf as t8_spzf,t8.zgzf as t8_zgzf,t8.zdzf as t8_zdzf,t8.volume as t8_volume,t8.amount as t8_amount,t8.pre_close as t8_pre_close,t8.qjzf as t8_qjzf,t8.stzf as t8_stzf,t8.kxzt as t8_kxzt,t8.kp as t8_kp,t8.wp as t8_wp ,t8.sx as t8_sx,t8.xx as t8_xx,t8.ln as t8_ln,t8.zrlnb as t8_zrlnb,t8.turnover_ratio as t8_turnover_ratio,t8.change as t8_change,t8.change_pct as t8_change_pct,
         |t9.trade_date as t9_trade_date,t9.bk as t9_bk,t9.sfzt as t9_sfzt,t9.cjzt as t9_cjzt, t9.open as t9_open , t9.close as t9_close ,t9.high as t9_high,t9.low as t9_low,t9.kpzf as t9_kpzf,t9.spzf as t9_spzf,t9.zgzf as t9_zgzf,t9.zdzf as t9_zdzf,t9.volume as t9_volume,t9.amount as t9_amount,t9.pre_close as t9_pre_close,t9.qjzf as t9_qjzf,t9.stzf as t9_stzf,t9.kxzt as t9_kxzt,t9.kp as t9_kp,t9.wp as t9_wp ,t9.sx as t9_sx,t9.xx as t9_xx,t9.ln as t9_ln,t9.zrlnb as t9_zrlnb,t9.turnover_ratio as t9_turnover_ratio,t9.change as t9_change,t9.change_pct as t9_change_pct

         | from ta4 as t0
         | left join ta4 as t1 on t0.row+1=t1.row and t0.stock_code=t1.stock_code
         | left join ta4 as t2 on t0.row+2=t2.row and t0.stock_code=t2.stock_code
         | left join ta4 as t3 on t0.row+3=t3.row and t0.stock_code=t3.stock_code
         | left join ta4 as t4 on t0.row+4=t4.row and t0.stock_code=t4.stock_code
         | left join ta4 as t5 on t0.row+5=t5.row and t0.stock_code=t5.stock_code
         | left join ta4 as t6 on t0.row+6=t6.row and t0.stock_code=t6.stock_code
         | left join ta4 as t7 on t0.row+7=t7.row and t0.stock_code=t7.stock_code
         | left join ta4 as t8 on t0.row+8=t8.row and t0.stock_code=t8.stock_code
         | left join ta4 as t9 on t0.row+9=t9.row and t0.stock_code=t9.stock_code

         |""".stripMargin)

    df5.createTempView("ta5")

    spark.sql("select * from ta5")
      .repartition(8).write.mode("overwrite").partitionBy("trade_date_month")
      .parquet(s"file:///D:\\${ParameterSet.data_content}\\gpsj_hs_10days_increment")

    //TODO 更新三个数据集除第一个月的数据的其他数据
    FileTools.dataFrame2Increment(spark:SparkSession,"gpsj_hs_10days","gpsj_hs_10days_increment",m2,m3)
  }


  /**
   * 【先删后加】增量更新，将更新的分区数据移动（改名），然后更新新的数据按照分区增量更
   * 注意：犹豫宽表
   * @param spark
   * @param df
   * @param CompleteSinkPath
   * @param IncrementSinkPath
   */
  def updateGpsjIncrement(spark:SparkSession,df:DataFrame,CompleteSinkPath:String,IncrementSinkPath:String)={
    val date = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())

    df.distinct().repartition(1).write.mode("overwrite")
      .partitionBy("trade_date_month")
      .parquet(s"file:///D:\\${ParameterSet.data_content}\\$IncrementSinkPath")

    //读取数据，确定分区
    val df2 = spark.read.parquet(s"file:///D:\\${ParameterSet.data_content}\\$IncrementSinkPath")
    val pathArray = df2.withColumn("file_name", input_file_name())
      .select("file_name")
      .distinct().collect()
    pathArray.foreach(f=>{
      //source源目录，sink目标目录，archieve归档目录
      val path = f.getAs[String]("file_name").replaceAll("file:///","")
      val source = path.split("/").dropRight(1).mkString("/")
      val sink = path.split("/").dropRight(1).mkString("/").replaceAll(IncrementSinkPath,CompleteSinkPath)
      val achieve = path.split("/").dropRight(1).mkString("/").replaceAll(IncrementSinkPath,CompleteSinkPath).replaceAll(CompleteSinkPath,"achieve/"+CompleteSinkPath+"_"+date)
      val achieveTarget = achieve.split("/").dropRight(1).mkString("/")
      //判断路径是否存在，如果存在则迁移到归档去
//      println(source)
//      println(sink)
//      println(achieve)
//      println(achieveTarget)

      FileTools.incrementRename(source,sink,achieve)

    })
  }
}
