package sparktask.listener

import org.apache.spark.sql.SparkSession

object SparkProgressDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("GlobalProgressDemo")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val progressListener = new GlobalProgressListener
    spark.sparkContext.addSparkListener(progressListener)

    // 启动进度条线程
    ProgressBar.show(progressListener)

    // 示例 Spark 作业（包含多个 Stage）
    spark.range(0, 10000000)
      .repartition(10)
      .write.mode("overwrite").parquet("file:///C:\\Users\\Administrator\\Desktop\\gs2024\\testlistener1")

    spark.range(0, 5000000)
      .groupBy("id").count()
      .write.mode("overwrite").parquet("file:///C:\\Users\\Administrator\\Desktop\\gs2024\\testlistener2")

    // 等待所有任务完成
    progressListener.awaitCompletion()
    spark.close()
  }
}
