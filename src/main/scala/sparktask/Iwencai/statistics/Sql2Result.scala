package sparktask.Iwencai.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Sql2Result {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
    val spark = SparkSession
      .builder()
      .appName("GSDemo2")
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark.read.parquet("file:///F:\\gs\\gssj").createTempView("ta1")
    spark.sql("select t1.*,t2.* from (select *,row_number() over(partition by stock_code order by stock_code,trade_date) as row,row_number() over(partition by stock_code order by stock_code,trade_date)-1 as row1,row_number() over(partition by stock_code order by stock_code,trade_date)-2 as row2 from ta1 where trade_date>'2023-12-31' order by stock_code,trade_date) as t1 left join (select *,row_number() over(partition by stock_code order by stock_code,trade_date) as row,row_number() over(partition by stock_code order by stock_code,trade_date)-1 as row1,row_number() over(partition by stock_code order by stock_code,trade_date)-2 as row2 from ta1 where trade_date>'2023-12-31' order by stock_code,trade_date) as t2 on t1.stock_code=t2.stock_code and t1.row=t2.row1 ").show(false)



    spark.close()
  }
}
