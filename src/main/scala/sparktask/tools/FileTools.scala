package sparktask.tools

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.input_file_name

object FileTools {


  /**
   * 增量替换，并将全量历史目录迁移到归档目录中，将增量目录替换到全量目录中
   * @param source
   * @param sink
   * @param achieve
   */
  def incrementRename(source:String,sink:String,achieve:String): Unit ={
    val sourceDir = new File(source)
    val sinkDir = new File(sink)

    println(achieve.split("/").dropRight(1).mkString("/"))

    //创建归档目录，并将目录移动到该目录下
    val targetDirPath = achieve.split("/").dropRight(1).mkString("/")
    val targetDir = new File(targetDirPath)
    if (!targetDir.exists()) {
      targetDir.mkdirs() // 如果目标目录不存在，则创建它
    }else{
      targetDir.delete()
      targetDir.mkdirs()
    }
    //判断sink目录是否存在，如果不存在则创建
    if(!sinkDir.exists()){
      sinkDir.mkdirs()
    }

    val newSubDir = new File(targetDir, sinkDir.getName())
    if (!newSubDir.exists()) {
      if (sinkDir.renameTo(newSubDir)) {
        println(s"目录 $sink 已成功移动到 $targetDirPath 下，成为其子目录")
      } else {
        println(s"移动目录 $sink 失败，可能存在权限问题或其他异常情况")
      }
    }
    //将源目录移动到目标目录
    if(!sinkDir.exists()){
      if (sourceDir.renameTo(sinkDir)) {
        println(s"目录 $source 已成功移动到 $sink 下，成为其子目录")
      } else {
        println(s"移动目录 $source 失败，可能存在权限问题或其他异常情况")
      }
    }
  }

  /**
   * 获取增量路径的目录下的分区数组，遍历数组
   * 并执行incrementRename
   * 注意：T+1情况下需要对最早的月份删除，因为增量数据中最早的月份数据不全
   * @param spark
   * @param CompleteSinkPath
   * @param IncrementSinkPath
   * @param m2  上一月
   * @param m3  当前最新月
   */
  def dataFrame2Increment(spark:SparkSession,CompleteSinkPath:String,IncrementSinkPath:String,m2:String,m3:String): Unit ={
  val date = new SimpleDateFormat("yyyyMMddMMhhss").format(new Date())
  //读取数据，确定分区(m1数据缺失，所以不更新)
  val df2 = spark.read.parquet(s"file:///D:\\gsdata\\$IncrementSinkPath\\trade_date_month=$m2",s"file:///D:\\gsdata\\$IncrementSinkPath\\trade_date_month=$m3")
  val pathArray = df2.withColumn("file_name", input_file_name())
    .select("file_name")
    .distinct().collect()

    df2.withColumn("file_name", input_file_name())
      .select("file_name")
      .distinct().show(false)
  pathArray.foreach(f=>{
    //source源目录，sink目标目录，archieve归档目录
    val path = f.getAs[String]("file_name").replaceAll("file:///","")
    val source = path.split("/").dropRight(1).mkString("/")
    val sink = path.split("/").dropRight(1).mkString("/").replaceAll(IncrementSinkPath,CompleteSinkPath)
    val achieve = path.split("/").dropRight(1).mkString("/").replaceAll(IncrementSinkPath,CompleteSinkPath).replaceAll(CompleteSinkPath,"achieve/"+CompleteSinkPath+"_"+date)
    val achieveTarget = achieve.split("/").dropRight(1).mkString("/")
    //判断路径是否存在，如果存在则迁移到归档去
    println(source)
    println(sink)
    println(achieve)
    println(achieveTarget)

    FileTools.incrementRename(source,sink,achieve)

  })
}

}
