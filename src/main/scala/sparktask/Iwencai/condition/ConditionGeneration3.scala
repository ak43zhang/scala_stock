package sparktask.Iwencai.condition

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * I问财条件拼接
 */
object ConditionGeneration3 {
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

    val startm = System.currentTimeMillis()

    val date = "2024-07-01"
    condition2result(spark,date)

    val endm = System.currentTimeMillis()
    println("共耗时："+(endm-startm)/1000+"秒")
    spark.close()
  }


  def condition2result(spark:SparkSession,date:String): Unit ={

    println("当前条件生成日期为："+date)
    println("---------------")

    val dmdf = spark.read.text(s"file:///C:\\Users\\Administrator\\Desktop\\gs2024\\条件选股2\\${date.replaceAll("-","")}.txt")
      .filter("value like 'SZ%' or value like 'SH%'")
      .withColumn("value",substring(col("value"),3,6))
    println(dmdf.count())
    dmdf.createOrReplaceTempView("ta1")
    //    dmdf.show()

    val day20df = spark.read.parquet("file:///D:\\gsdata\\gpsj_hs_20days\\trade_date_month=2024*")
    day20df.createOrReplaceTempView("ta2")
    //    day20df.show()

    var zfyz = 5
    var resdf:DataFrame = null

    for(i<-5 to 9){
      zfyz=i
//      println("-------当前涨幅因子为："+zfyz)
      val middf = spark.sql(s"""select ta2.* from ta1 left join ta2 on ta1.value=ta2.stock_code where t10_trade_date='${date}' and t10_zgzf>=$zfyz""")
      middf.createOrReplaceTempView("ta3")

      /**
       * 涨停数量
       * 炸板数量
       * 最高涨幅超过5的数量
       * 最高涨幅超过5 最高涨幅-收盘涨幅 hc:回撤
       */
      //    spark.sql(s"select stock_code,t10_sfzt,t10_cjzt,t10_zgzf,t10_spzf,t11_kpzf,t11_spzf,t11_zgzf from ta3  order by t10_zgzf desc").show(500)


      val zfdf = spark.sql(s"select $zfyz as `涨幅因子`,sum(t10_sfzt) as `涨停数量`,sum(t10_cjzt) as `炸板数量`,sum(if(t10_zgzf>=$zfyz,1,0)) as `大于涨幅因子数量` from ta3")
//        zfdf.show()

      val mid2df = spark.sql(
        s"""select t10_spzf-$zfyz as `当日收益`,
           |t10_spzf-$zfyz+t11_kpzf as `竞价收益`,
           |t10_spzf-$zfyz+t11_zgzf as `最大收益`,
           |t10_spzf-$zfyz+t11_zdzf as `最低收益`,
           |t10_spzf-t10_zgzf as `回撤`,t11_kpzf,t11_zgzf,t11_spzf from ta3 order by t10_zgzf desc""".stripMargin)
      //      mid2df.show()
      mid2df.createOrReplaceTempView("ta4")

      val sydf = spark.sql(
        s"""select $zfyz as `涨幅因子`,
          |sum(if(`竞价收益`>=0,1,0)) as `竞价获利数量`,
          |sum(if(`竞价收益`<0,1,0)) as `竞价亏损数量`,
          |round(sum(if(`竞价收益`>=0,1,0))/count(1),2) as `竞价获利百分比`,
          |sum(if(`最大收益`>=0,1,0)) as `最大获利数量`,
          |sum(if(`最大收益`<0,1,0)) as `最大亏损数量`,
          |round(sum(if(`最大收益`>=0,1,0))/count(1),2) as `最大获利百分比`
          | from ta4""".stripMargin)

//        sydf.show()

      val resdfmid = zfdf.crossJoin(sydf)
      if(resdf==null){
        resdf=resdfmid
      }else{
        resdf=resdf.union(resdfmid)
      }
    }

    resdf.show()


  }
}
