package sparktask.mysql.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 先通过Sql2Result一次筛选数据
 * 在通过该类二次筛选数据
 * 筛选条件：在压力位和支撑位之间，靠近支撑位更近一点
 *
 */
object Spark2Excel2Filter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
    val spark = SparkSession
      .builder()
      .appName("Spark2Excel2Filter")
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import org.apache.spark.sql.{DataFrame, SparkSession}

    val fileName = "条件20241212_2涨停"
    val dmName = "dm20241212_涨停"

    val excelDF: DataFrame = spark.read
      .format("com.crealytics.spark.excel")
      //      .option("sheetName", "Sheet1") // 读取的Sheet页
      .option("header", "true") // 第一行不作为表头,如果为true则作为表头
      .option("dataAddress", s"$fileName!A1:BH5000") // 'page1'Sheet页名称(也可以选用'sheetName'的方式进行配置),A3代表从第几行读取(3则代表从第三行),E6代表行的范围
      .option("treatEmptyValuesAsNulls", "true") // 空值是否为作为null
      .option("inferSchema", "true")
      .load(s"file:///C:\\Users\\Administrator\\Desktop\\gs2024\\条件选股\\$fileName.xlsx") // 如果是本地文件需要标注'file:///实际路径'因为spark会默认将HDFS作为文件系统
    //        val excelHeader = Seq("id2", "name2", "age2", "hobbit2", "phoneNum") // 自定义表头名称
    //        val frameDF = excelDF.toDF(excelHeader: _*)
    //        frameDF.show()


    excelDF.show(false)

    //压力位支撑位距离
    excelDF.createTempView("ta1")

    /**
     * 当今日结束则选择昨收，今日预备选择收盘价
     */
    //昨收
//    val df = spark.sql(
//      """
//        select *,yl/zc as cb from (
//        select substr(`代码`,-6) as dm,`涨幅`*100 as zf,`竞价涨幅%` as jjzf,`最大涨幅` as zdzf,`实体涨幅`*100 as stzf,`金叉个数` as jcsl,
//        if(`压力位`>`昨收`,1,0) as gy,if(`压力位`>`昨收`,`压力位`-`昨收`,`昨收`-`压力位`) as yl,
//        if(`昨收`>`支撑位`,1,0) as dy,if(`昨收`>`支撑位`,`昨收`-`支撑位`,`支撑位`-`昨收`) as zc
//        from ta1
//        ) where gy=1 and dy=1 and yl/zc>=1
//        order by jcsl desc ,yl/zc desc
//        """.stripMargin)

    //收盘价,目前插比定在1，根据20241209数据可知，越大，胜率越高
        val df = spark.sql(
          """
            select *,yl/zc as cb from (
            select substr(`代码`,-6) as dm,`涨幅` as zf,`金叉个数` as jcsl,
            if(`压力位`>`收盘价`,1,0) as gy,if(`压力位`>`收盘价`,`压力位`-`收盘价`,`收盘价`-`压力位`) as yl,
            if(`收盘价`>`支撑位`,1,0) as dy,if(`收盘价`>`支撑位`,`收盘价`-`支撑位`,`支撑位`-`收盘价`) as zc
            from ta1
            ) where gy=1 and dy=1 and yl/zc>=1
            order by yl/zc desc
            """.stripMargin)

    df.show(1000)

    val df2 = df.select("dm")

//    df2.repartition(1).write.mode("overwrite").text(s"file:///C:\\Users\\Administrator\\Desktop\\gs2024\\$dmName")


    spark.close()
  }
}
