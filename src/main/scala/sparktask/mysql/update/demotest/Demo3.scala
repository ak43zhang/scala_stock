package sparktask.mysql.update.demotest

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
 * 用于测试  连续涨停
 */
object Demo3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.local.dir","D:\\Temp")
    val spark = SparkSession
      .builder()
      .appName("Demo3")
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val df = spark.sql("select 'a' as s,1 as z union all select 'a',1  union all select 'a',1  union all select 'a',0  union all select 'a',0  union all select 'a',1 union all select 'a',1 union all select 'a',0")
    df.createTempView("ta1")
    df.show()
    var flag:Int = -1
    val df2 = df.flatMap(f=>{
      val arr = new ArrayBuffer[(String,Int,Int)]
      val s = f.getAs[String]("s")
      val z = f.getAs[Int]("z")

     if(flag== -1 || flag==z){
       flag = z
      }else{
       flag=flag+1
      }
      arr+=((s,z,flag))
    })
    df2.show()

    spark.close()
  }


}
