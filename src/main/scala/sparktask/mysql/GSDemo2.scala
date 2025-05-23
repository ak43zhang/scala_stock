package sparktask.mysql

import java.util.Properties
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object GSDemo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled","true")
    val spark = SparkSession
      .builder()
      .appName("GSDemo2")
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val url = "jdbc:mysql://localhost:3306/gs"
    val gpsj_day = "data2024_gpsj_day"
    val t0108 = "data2024_ss_fshq_0108"
    val t0109 = "data2024_ss_fshq_0109"
    val t0110 = "data2024_ss_fshq_0110"
    val t0111 = "data2024_ss_fshq_0111"
    val t0112 = "data2024_ss_fshq_0112"
    val t0115 = "data2024_ss_fshq_0115"
    val t0116 = "data2024_ss_fshq_0116"
    val t0117 = "data2024_ss_fshq_0117"
    val t0118 = "data2024_ss_fshq_0118"
    val t0119 = "data2024_ss_fshq_0119"
    val dzgpssgn = "data2024_dzgpssgn"
    val gnzsxx = "data2024_gnzsxx"


    val driver = "com.mysql.cj.jdbc.Driver"
    val user = "root"
    val pwd = "123456"

    val properties = new Properties()
    properties.setProperty("user",user)
    properties.setProperty("password",pwd)
    properties.setProperty("url",url)
    properties.setProperty("driver",driver)


//    spark.read.jdbc(url, tablename, properties).createTempView("t0112_1")
//    val df = spark.sql("select t1.* from (select * from t0112_1 where volume>1000000 and trade_time>'2024-01-08 09:35' and trade_time<'2024-01-08 14:30' and change>0) as t1 left join (select * from t0112_1 where trade_time ='2024-01-08 09:31:00') as t2 on t1.stock_code=t2.stock_code where t1.volume>t2.volume")
//    df.show(1000)
//    df.createTempView("t0112_2")
//    val df2 = spark.sql("select stock_code from t0112_2").distinct()
//      df2.createTempView("t0112_3")
//    df2.where("stock_code like '60%' or stock_code like '00%'").show(100)
//    spark.read.jdbc(url, dzgpssgn, properties).createTempView("t0112_4")
//    spark.sql("select concept_code,name,count(1) from t0112_3 left join t0112_4 on t0112_4.stock_code=t0112_3.stock_code group by concept_code,name order by count(1) desc")
//      .show(300,false)
//        df.repartition(20).write.mode("overwrite").parquet("file:///F:\\gs\\gssj")

    //量能数据
//    spark.read.jdbc(url, t0109, properties).createTempView("t0112_1")
//    val df = spark.sql("select t1.* from (select * from t0112_1 where volume>1000000 and change_pct>2 and trade_time>'2024-01-09 09:35' and trade_time<'2024-01-09 14:30' and change>0) as t1 left join (select * from t0112_1 where trade_time ='2024-01-09 09:31:00') as t2 on t1.stock_code=t2.stock_code where t1.volume>t2.volume")
//    val df2 = df.where("stock_code like '60%' or stock_code like '00%'")
//    println(df2.count())
//    df2.show(1000,false)


    //涨停板数据

    val zfbfb = "6"

    spark.read.jdbc(url, t0108, properties).where(s"trade_time='2024-01-08 15:00:00' and change_pct >$zfbfb").createTempView("t0108_1")
    spark.read.jdbc(url, dzgpssgn, properties).createTempView("t0108_2")
    spark.sql("select t0108_1.*,t0108_2.* from t0108_1 left join t0108_2 on t0108_1.stock_code=t0108_2.stock_code where t0108_2.stock_code is not null").createTempView("t0108_3")
    spark.sql("select name,count(1) as t0108c from t0108_3 group by name order by count(1) desc").createTempView("t0108_4")

    spark.read.jdbc(url, t0109, properties).where(s"trade_time='2024-01-09 15:00:00' and change_pct >$zfbfb").createTempView("t0109_1")
    spark.read.jdbc(url, dzgpssgn, properties).createTempView("t0109_2")
    spark.sql("select t0109_1.*,t0109_2.* from t0109_1 left join t0109_2 on t0109_1.stock_code=t0109_2.stock_code where t0109_2.stock_code is not null").createTempView("t0109_3")
    spark.sql("select name,count(1) as t0109c from t0109_3 group by name order by count(1) desc").createTempView("t0109_4")

    spark.read.jdbc(url, t0110, properties).where(s"trade_time='2024-01-10 15:00:00' and change_pct >$zfbfb").createTempView("t0110_1")
    spark.read.jdbc(url, dzgpssgn, properties).createTempView("t0110_2")
    spark.sql("select t0110_1.*,t0110_2.* from t0110_1 left join t0110_2 on t0110_1.stock_code=t0110_2.stock_code where t0110_2.stock_code is not null").createTempView("t0110_3")
    spark.sql("select name,count(1) as t0110c from t0110_3 group by name order by count(1) desc").createTempView("t0110_4")

    spark.read.jdbc(url, t0111, properties).where(s"trade_time='2024-01-11 15:00:00' and change_pct >$zfbfb").createTempView("t0111_1")
    spark.read.jdbc(url, dzgpssgn, properties).createTempView("t0111_2")
    spark.sql("select t0111_1.*,t0111_2.* from t0111_1 left join t0111_2 on t0111_1.stock_code=t0111_2.stock_code where t0111_2.stock_code is not null").createTempView("t0111_3")
    spark.sql("select name,count(1) as t0111c from t0111_3 group by name order by count(1) desc").createTempView("t0111_4")

    spark.read.jdbc(url, t0112, properties).where(s"trade_time='2024-01-12 15:00:00' and change_pct >$zfbfb").createTempView("t0112_1")
    spark.read.jdbc(url, dzgpssgn, properties).createTempView("t0112_2")
    spark.sql("select t0112_1.*,t0112_2.* from t0112_1 left join t0112_2 on t0112_1.stock_code=t0112_2.stock_code where t0112_2.stock_code is not null").createTempView("t0112_3")
    spark.sql("select name,count(1) as t0112c from t0112_3 group by name order by count(1) desc").createTempView("t0112_4")

    spark.read.jdbc(url, t0115, properties).where(s"trade_time='2024-01-15 15:00:00' and change_pct >$zfbfb").createTempView("t0115_1")
    spark.read.jdbc(url, dzgpssgn, properties).createTempView("t0115_2")
    spark.sql("select t0115_1.*,t0115_2.* from t0115_1 left join t0115_2 on t0115_1.stock_code=t0115_2.stock_code where t0115_2.stock_code is not null").createTempView("t0115_3")
    spark.sql("select name,count(1) as t0115c from t0115_3 group by name order by count(1) desc").createTempView("t0115_4")

    spark.read.jdbc(url, t0116, properties).where(s"trade_time='2024-01-16 15:00:00' and change_pct >$zfbfb").createTempView("t0116_1")
    spark.read.jdbc(url, dzgpssgn, properties).createTempView("t0116_2")
    spark.sql("select t0116_1.*,t0116_2.* from t0116_1 left join t0116_2 on t0116_1.stock_code=t0116_2.stock_code where t0116_2.stock_code is not null").createTempView("t0116_3")
    spark.sql("select name,count(1) as t0116c from t0116_3 group by name order by count(1) desc").createTempView("t0116_4")

    spark.read.jdbc(url, t0117, properties).where(s"trade_time='2024-01-17 15:00:00' and change_pct >$zfbfb").createTempView("t0117_1")
    spark.read.jdbc(url, dzgpssgn, properties).createTempView("t0117_2")
    spark.sql("select t0117_1.*,t0117_2.* from t0117_1 left join t0117_2 on t0117_1.stock_code=t0117_2.stock_code where t0117_2.stock_code is not null").createTempView("t0117_3")
    spark.sql("select name,count(1) as t0117c from t0117_3 group by name order by count(1) desc").createTempView("t0117_4")

    spark.read.jdbc(url, t0118, properties).where(s"trade_time='2024-01-18 15:00:00' and change_pct >$zfbfb").createTempView("t0118_1")
    spark.read.jdbc(url, dzgpssgn, properties).createTempView("t0118_2")
    spark.sql("select t0118_1.*,t0118_2.* from t0118_1 left join t0118_2 on t0118_1.stock_code=t0118_2.stock_code where t0118_2.stock_code is not null").createTempView("t0118_3")
    spark.sql("select name,count(1) as t0118c from t0118_3 group by name order by count(1) desc").createTempView("t0118_4")

    spark.read.jdbc(url, t0119, properties).where(s"trade_time='2024-01-19 15:00:00' and change_pct >$zfbfb").createTempView("t0119_1")
    spark.read.jdbc(url, dzgpssgn, properties).createTempView("t0119_2")
    spark.sql("select t0119_1.*,t0119_2.* from t0119_1 left join t0119_2 on t0119_1.stock_code=t0119_2.stock_code where t0119_2.stock_code is not null").createTempView("t0119_3")
    spark.sql("select name,count(1) as t0119c from t0119_3 group by name order by count(1) desc").createTempView("t0119_4")

    spark.read.jdbc(url, gnzsxx, properties).createTempView("qq")

    val res = spark.sql("""select qq.name,t0108c,t0109c,t0110c,t0111c,t0112c,t0115c,t0116c,t0117c,t0118c,t0119c from qq
          left join t0108_4 on qq.name=t0108_4.name
          left join t0109_4 on qq.name=t0109_4.name
          left join t0110_4 on qq.name=t0110_4.name
          left join t0111_4 on qq.name=t0111_4.name
          left join t0112_4 on qq.name=t0112_4.name
          left join t0115_4 on qq.name=t0115_4.name
          left join t0116_4 on qq.name=t0116_4.name
          left join t0117_4 on qq.name=t0117_4.name
          left join t0118_4 on qq.name=t0118_4.name
          left join t0119_4 on qq.name=t0119_4.name
          order by t0118c desc""")

    res.show(1000,false)

    spark.close()
  }
}
