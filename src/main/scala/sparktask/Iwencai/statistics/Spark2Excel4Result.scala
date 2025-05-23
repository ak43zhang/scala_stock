package sparktask.Iwencai.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 用于分析
 */
object Spark2Excel4Result {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
    val spark = SparkSession
      .builder()
      .appName("Spark2Excel4Result")
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import org.apache.spark.sql.{DataFrame, SparkSession}


        val excelDF: DataFrame = spark.read
          .format("com.crealytics.spark.excel")
          //      .option("sheetName", "Sheet1") // 读取的Sheet页
          .option("header", "true") // 第一行不作为表头,如果为true则作为表头
          .option("dataAddress", "'条件20241212_first_昨收'!A1:BG5000") // 'page1'Sheet页名称(也可以选用'sheetName'的方式进行配置),A3代表从第几行读取(3则代表从第三行),E6代表行的范围
          .option("treatEmptyValuesAsNulls", "true") // 空值是否为作为null
          .option("inferSchema", "true")
          .load("file:///C:\\Users\\Administrator\\Desktop\\gs2024\\条件选股\\条件20241212_first_昨收.xlsx") // 如果是本地文件需要标注'file:///实际路径'因为spark会默认将HDFS作为文件系统
//        val excelHeader = Seq("id2", "name2", "age2", "hobbit2", "phoneNum") // 自定义表头名称
//        val frameDF = excelDF.toDF(excelHeader: _*)
//        frameDF.show()


    excelDF.show(false)

    //压力位支撑位距离
    excelDF.createTempView("ta1")

    //获取竞价前的数据条件，i问财不能处理的
//    val df = spark.sql(
//      """
//        |select substr(`代码`,-6) as dm from ta1
//        |where `压力位`-`昨收`>`昨收`-`支撑位`
//        |""".stripMargin)
//
//    df.show()
//
//    df.printSchema()
//
//    println(df.count)
//    df.repartition(1).write.mode("overwrite").text("file:///C:\\Users\\Administrator\\Desktop\\gs2024\\dm20241205")
//    order by (`压力位`-`昨收`)/(`昨收`-`支撑位`) desc

    /**
     * dm 代码
     * zf 涨幅
     * gy 高压
     * dy 低压
     * yl 压力差
     * zc 支撑差
     * cb 差比
     */
    //昨收
    val df = spark.sql(
      """
        select *,yl/zc as cb from (
        select substr(`代码`,-6) as dm,`涨幅`*100 as zf,`竞价涨幅%` as jjzf,`最大涨幅` as zdzf,`实体涨幅`*100 as stzf,`金叉个数` as jcsl,
        if(`压力位`>`昨收`,1,0) as gy,if(`压力位`>`昨收`,`压力位`-`昨收`,`昨收`-`压力位`) as yl,
        if(`昨收`>`支撑位`,1,0) as dy,if(`昨收`>`支撑位`,`昨收`-`支撑位`,`支撑位`-`昨收`) as zc
        from ta1
        ) where gy=1 and dy=1
        order by jcsl desc ,yl/zc desc
        """.stripMargin)

    //收盘价
//    val df = spark.sql(
//      """
//        select *,yl/zc as cb from (
//        select substr(`代码`,-6) as dm,`涨幅` as zf,
//        if(`压力位`>`收盘价`,1,0) as gy,if(`压力位`>`收盘价`,`压力位`-`收盘价`,`收盘价`-`压力位`) as yl,
//        if(`收盘价`>`支撑位`,1,0) as dy,if(`收盘价`>`支撑位`,`收盘价`-`支撑位`,`支撑位`-`收盘价`) as zc
//        from ta1
//        ) where gy=1 and dy=1
//        order by zf desc
//        """.stripMargin)
//and yl/zc>1
    df.createTempView("ta2")

    df.show(2000,false)

    val df2 = df.select("dm")

    /**
     * 以20241205为例子
     * cb小于1时，涨幅大于0的297个，涨幅小于0的79个，小于0的比例较大
     * cb大于1时，涨幅大于0的562个，涨幅小于0的61个，小于0的比例较小
     *
     */

    var combinedDF: DataFrame = null

      for(i<-1.0 to 5.0 by 0.1){
        val r1 = spark.sql(
          s"""select r1/(r1+r2+r3+r4) as r1b,r2/(r1+r2+r3+r4) as r2b,r3/(r1+r2+r3+r4) as r3b,r4/(r1+r2+r3+r4) as r4b ,(r3/(r1+r2+r3+r4))/(r4/(r1+r2+r3+r4)) as bfb,
            |r1/(r1+r2) as r1a,r2/(r1+r2) as r2a,r3/(r3+r4) as r3a,r4/(r3+r4) as r4a ,
            |$i as i
            |from (select
            |sum(if(cb<$i and zf>0,1,0)) as r1,
            |sum(if(cb<$i and zf<0,1,0)) as r2,
            |sum(if(cb>$i and zf>0,1,0)) as r3,
            |sum(if(cb>$i and zf<0,1,0)) as r4
            | from ta2)""".stripMargin)

        if(combinedDF==null){
          combinedDF=r1
        }else{
          combinedDF=combinedDF.union(r1)
        }
      }

    combinedDF.show(100)
//    val r1 = spark.sql(
//      """select r1/(r1+r2+r3+r4) as r1b,r2/(r1+r2+r3+r4) as r2b,r3/(r1+r2+r3+r4) as r3b,r4/(r1+r2+r3+r4) as r4b from (select
//        |sum(if(cb<1 and zf>0,1,0)) as r1,
//        |sum(if(cb<1 and zf<0,1,0)) as r2,
//        |sum(if(cb>1 and zf>0,1,0)) as r3,
//        |sum(if(cb>1 and zf<0,1,0)) as r4
//        | from ta2)""".stripMargin)
//      r1.show()




    //    df.printSchema()

//    println(df.count)
//    df2.repartition(1).write.mode("overwrite").text("file:///C:\\Users\\Administrator\\Desktop\\gs2024\\dm20241209")

    spark.close()
  }
}
