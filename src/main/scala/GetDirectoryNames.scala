import java.io.File

object GetDirectoryNames {
  def main(args: Array[String]): Unit = {
    // 设置a目录的路径，这里使用相对路径或绝对路径
    val aDirPath = "H:\\mysql_backup\\gs\\parquet"  // 相对路径
    // val aDirPath = "C:\\path\\to\\a"  // 绝对路径
    // val aDirPath = "/path/to/a"       // Linux风格的路径在Windows上也能工作

    val directory = new File(aDirPath)

    // 检查目录是否存在且是一个目录
    if (directory.exists() && directory.isDirectory) {
      // 获取所有子目录
      val subDirs = directory.listFiles().filter(_.isDirectory)

      // 提取目录名
      val dirNames = subDirs.map(_.getName)

      // 打印所有目录名
      println("找到的MySQL表名（目录名）：")
      dirNames.foreach(println)

      // 返回目录名列表
      println(s"共找到 ${dirNames.length} 个目录")

      // 如果需要返回List[String]
      val dirNamesList = dirNames.toList
    } else {
      println(s"目录 '$aDirPath' 不存在或不是一个目录")
    }
  }
}