package sparktask.test

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

/**
 * 转债筛选
 * 流动性
 * 风险
 */
object TestBond {
  def main(args: Array[String]): Unit = {
    val startm = System.currentTimeMillis()
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.crossJoin.enabled", "true")
      // 增加shuffle分区数
      .set("spark.sql.shuffle.partitions", "10")
      .set("spark.sql.broadcastTimeout","60000")
      .set("spark.driver.memory", "4g")
      // 增加JDBC并行任务数
      .set("spark.jdbc.parallelism", "10")
      .set("spark.local.dir", "D:\\SparkTemp")

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val properties = getMysqlProperties()
    val url = properties.getProperty("url")

    val bond_df = spark.read.jdbc(url,"data_bond",properties)

    val bond_qs_df = spark.read.jdbc(url,"data_bond_qs_jsl",properties)
      .select("代码","现价","规模","剩余规模","强赎触发比","强赎触发价","转股价","正股价","强赎价")

    val filteredResults = filterConvertibleBonds(bond_df, bond_qs_df)
    filteredResults.show(1000)

    val endm = System.currentTimeMillis()
    println("共耗时：" + (endm - startm) / 1000 + "秒")
    spark.close()
  }

  def filterConvertibleBonds(df1: DataFrame, df2: DataFrame): DataFrame = {

    // 数据关联
    val joinedDF = df1.alias("a")
      .join(df2.alias("b"),
        substring(col("a.债券代码"), 1, 6) === substring(col("b.代码"), 1, 6),
        "inner")

    // 动态计算关键指标（核心修改）
    val dfWithCalculations = joinedDF
      // 计算强赎触发比：正股价 / 强赎触发价
      .withColumn("calc_强赎触发比",
        // 防止除零错误，并处理空值
        when(col("b.强赎触发价").isNotNull && col("b.强赎触发价") =!= 0,
          col("b.正股价") / col("b.强赎触发价"))
          .otherwise(null))  // 无法计算时设为null
      // 可选：计算转股价值，用于更精确的溢价率分析（如需要）
      .withColumn("calc_转股价值", col("b.正股价") * 100 / col("a.转股价")) // 公式：(100/转股价)*正股价
      .withColumn("上市日期", to_date(col("a.上市时间"), "yyyy-MM-dd"))
//      .withColumn("剩余年限", datediff(col("上市日期"), current_date()) / 365.0)
      .withColumn("calc_规模缩减比", col("b.剩余规模") / col("b.规模"))
//      .orderBy(col("剩余年限").desc)

    // 1. 第一层滤网：价格与溢价率风险
    val filter1 = dfWithCalculations.filter(
      (col("b.现价").lt(200) && col("a.转股溢价率").lt(50) && col("a.转股溢价率").gt(0))
    )

    // 2. 第二层滤网：强赎与条款风险（使用动态计算值）
    // 过滤条件：计算出的强赎触发比 < 0.85 或 无法计算该比率
    val filter2 = filter1.filter(col("calc_强赎触发比").isNull || col("calc_强赎触发比").lt(0.85))

    // 3. 第三层滤网：流动性风险
    val filter3 = filter2.filter(
      col("b.剩余规模").geq(1.0)
    )

    // 4. 【优化核心】第四层滤网：专业期限与弹性抗风险过滤
    val filter4 = filter3
      // 4.1 严格规避临期债：基于上市时间推算剩余年限
      // 假设“a.上市时间”字段格式为“yyyy-MM-dd”，计算距今是否大于2年
//      .filter(col("剩余年限").geq(1.5) || col("剩余年限").isNull) // 剔除剩余<2年或日期异常的债

      // 4.2 精准评估转股价值区间：剔除“股债双杀”和“强赎临界”品种
      .filter(col("calc_转股价值").between(90, 125)) // 最佳弹性区间
        // 转股价值90-125元的债：
        // >90：有足够股性跟随上涨，避免纯债性品种反应迟钝
        // <125：远离130元的强赎线，留有安全空间

      // 4.3 动态规模稳定性检查：警惕大规模转股后的流动性萎缩
      .filter(col("calc_规模缩减比").geq(0.3))
      // 剩余规模不足发行规模30%的债，可能已被大量转股，
      // 流动性会快速下降，且发行人强促转股意愿强烈

      // 4.4 基于市场状态的溢价率弹性过滤（专业核心）
      // 原则：牛市看股性（溢价率可稍高），熊市看债性（溢价率要低且价格要低）
      // 此处以“转股价值”作为市场情绪的代理变量
//      .filter(
//        when(col("calc_转股价值") < 105,  // 市场偏弱，执行严格标准
//          col("b.现价") < 115 && col("a.转股溢价率") < 25)
//          .otherwise( // 市场偏强，可适度放宽
//            col("a.转股溢价率") < 35)
//      )

      // 4.5 （可选高级项）规避“高价格+低转股价值”的估值陷阱
      // 此类债价格已透支，但正股未跟上，风险极高
//      .filter(!(col("b.现价") > 120 && col("calc_转股价值") < 105))


    /*dfWithCalculations.show(1000)
    println(dfWithCalculations.count())
    println(filter1.count())
    println(filter2.count())
    println(filter3.count())
    println(filter4.count())*/
    // 返回最终筛选结果
    filter4
  }


  def getMysqlProperties(): Properties ={
    val url = "jdbc:mysql://192.168.0.100:3306/gs"
    val driver = "com.mysql.cj.jdbc.Driver"
    val user = "root"
    val pwd = "123456"

    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", pwd)
    properties.setProperty("url", url)
    properties.setProperty("driver", driver)
    //    properties.setProperty("partitionColumn", "amount") // 选择一个分区列
    //    properties.setProperty("lowerBound", "10000") // 分区列的最小值
    //    properties.setProperty("upperBound", "1000000") // 分区列的最大值
    //    properties.setProperty("numPartitions", "8") // 设置分区数量
    // 关键：设置批量写入参数
    properties.setProperty("rewriteBatchedStatements", "true")
    properties.setProperty("useCompression", "true")
    properties.setProperty("cachePrepStmts", "true")
    properties.setProperty("prepStmtCacheSize", "250")
    properties.setProperty("prepStmtCacheSqlLimit", "2048")
    properties.setProperty("useServerPrepStmts", "true")

    properties
  }
}
