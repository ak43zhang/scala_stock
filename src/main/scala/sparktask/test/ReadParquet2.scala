package sparktask.test

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadParquet2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "10")
      .set("spark.driver.memory", "1g")
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

//    spark.read.jdbc(url, "fx002_v2", properties)
//      .where("trade_time='2024-01-02' and channel_position<=40")
//      .distinct()
//      .orderBy(col("trade_time").desc)
//      .show(100)

    spark.read.parquet("file:///C:\\Users\\Administrator\\Desktop\\gs2024\\result8")
      .withColumn("jjinitialCapital",col("jjinitialCapital").cast(FloatType))
      .withColumn("zginitialCapital",col("zginitialCapital").cast(FloatType))
      .withColumn("spinitialCapital",col("spinitialCapital").cast(FloatType))
      .withColumn("zdyinitialCapital",col("zdyinitialCapital").cast(FloatType))
      .createTempView("ta1")
    spark.sql(
      """select channel_position,count(1) from ta1
        |where spinitialCapital>10000
        |group by channel_position
        |order by count(1)  desc """.stripMargin).show(10000)

    spark.sql(
      """select t1_kpzf,count(1) from ta1
        |where spinitialCapital>10000
        |group by t1_kpzf
        |order by count(1)  desc """.stripMargin).show(10000)

    spark.sql(
      """select bugzdf,count(1) from ta1
        |where spinitialCapital>10000
        |group by bugzdf
        |order by count(1)  desc """.stripMargin).show(10000)

    spark.sql(
      """select row,count(1) from ta1
        |where spinitialCapital>10000
        |group by row
        |order by count(1)  desc """.stripMargin).show(10000)



//    spark.read.parquet("file:///C:\\Users\\Administrator\\Desktop\\gs2024\\result4*")
//      .where("day between '2023-04' and '2024-01' or day between '2024-05' and '2024-08'")
//      .withColumn("finalCapital",col("finalCapital").cast(FloatType))
//      .createTempView("ta1")
//    spark.sql(
//      """
//        |select t1_kpzf,sum(z),sum(z)/(sum(z)+sum(f)) from ta1 where row=1 and finalCapital>1 group by t1_kpzf order by t1_kpzf
//        |""".stripMargin).show(2000)
//
//    spark.sql(
//      """
//        |select bugzdf,sum(z),sum(f),sum(z)/(sum(z)+sum(f)) from ta1 where row=1 and finalCapital>1 group by bugzdf order by bugzdf
//        |""".stripMargin).show(2000)
//
//    spark.sql(
//      """
//        |select channel_position,sum(z),sum(f),sum(z)/(sum(z)+sum(f)) from ta1 where row=1 and channel_position>1 group by channel_position order by channel_position
//        |""".stripMargin).show(2000)

//    spark.sql(
//      """
//        |select  day,t1_kpzf,count(1) from ta1 group by day,t1_kpzf order by day,t1_kpzf
//        |""".stripMargin).show(10000)


//    spark.sql(
//      """
//        |select day,channel_position,sum(if(finalCapital>1,1,0)) as sl,sum(if(finalCapital<1,1,0)) as pl,sum(if(finalCapital>1,1,0))/sum(if(finalCapital!=1,1,0)) as zs from ta1
//        |where row=2  group by channel_position,day
//        |order by day,sum(if(finalCapital>1,1,0))/sum(if(finalCapital!=1,1,0)) desc
//        |
//        |""".stripMargin).where("zs>0.5").show(2000)

//    val middf1 = spark.sql(
//      """
//        |select day,channel_position,sum(if(zbfb>=0.5,1,0)) as sl,sum(if(zbfb<0.5,1,0)) as pl from ta1
//        |where row=8  group by channel_position,day
//        |order by day
//        |
//        |""".stripMargin).where("sl/pl>=1")
//    middf1.show(2000)
//    middf1.createTempView("ta2")
//
//    spark.sql(
//      """
//        |select channel_position,count(1) from ta2 group by channel_position order by count(1) desc
//        |""".stripMargin).show()


    //    spark.read.parquet("file:///D:\\gsdata\\pressure_support_calculator\\valid_results_pressure_advanced_dip_strategy")
//      .where("trade_time like '2023-02-03%'")
//      .orderBy(col("trade_time").desc).show()

    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }
}
