package sparktask.test

import java.io.File

object Test1 {
  def main(args: Array[String]): Unit = {
    val source = "D:/gsdata/gpsj_day_increment_hs/trade_date_month=2024-12"
    val sink = "D:/gsdata/gpsj_day_all_hs/trade_date_month=2024-12"
    val achieve = "D:/gsdata/achieve/gpsj_day_all_hs_20241219/trade_date_month=2024-12"




  }

  /**
   * 增量替换，并将全量历史目录迁移到归档目录中，将增量目录替换到全量目录中
   * @param source
   * @param sink
   * @param achieve
   */
  def incrementRename(source:String,sink:String,achieve:String): Unit ={
    val sourceDir = new File(source)
    val sinkDir = new File(sink)
    val file3 = new File(achieve)
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
    val newSubDir = new File(targetDir, sinkDir.getName())
    if (!newSubDir.exists()) {
      if (sinkDir.renameTo(newSubDir)) {
        println(s"目录 $sink 已成功移动到 $targetDirPath 下，成为其子目录")
      } else {
        println(s"移动目录 $sink 失败，可能存在权限问题或其他异常情况")
      }
    }
    if(!sinkDir.exists()){
      if (sourceDir.renameTo(sinkDir)) {
        println(s"目录 $source 已成功移动到 $sink 下，成为其子目录")
      } else {
        println(s"移动目录 $source 失败，可能存在权限问题或其他异常情况")
      }
    }
  }
}
