package sparktask.listener

/**
 * 控制台进度条工具
 */
object ProgressBar {
  def show(progressListener: GlobalProgressListener): Unit = {
    val runnable = new Runnable {
      override def run(): Unit = {
        val startTime = System.currentTimeMillis()
        var lastProgress = 0.0
        var smoothedProgress = 0.0
        var lastUpdateTime = startTime

        while (!progressListener.isAllDone) {
          val (totalTasks, completedTasks) = progressListener.getTaskStats
          val currentTime = System.currentTimeMillis()
          val elapsedSeconds = (currentTime - startTime) / 1000.0

          // 计算原始进度
          val rawProgress = if (totalTasks > 0) completedTasks.toDouble / totalTasks else 0.0

          // 进度平滑（加权平均减少抖动）
          val alpha = 0.3 // 平滑系数，越大则对新数据越敏感
          smoothedProgress = alpha * rawProgress + (1 - alpha) * smoothedProgress

          // 计算剩余时间（EMA 预测）
          val etaSeconds = if (smoothedProgress > 0.001) {
            val elapsedPerProgress = elapsedSeconds / smoothedProgress
            (elapsedPerProgress * (1 - smoothedProgress)).toInt
          } else -1

          // 格式化时间显示
          val elapsedStr = formatTime(elapsedSeconds.toInt)
          val etaStr = if (etaSeconds >= 0) formatTime(etaSeconds) else "--"

          // 绘制进度条
          val barLength = 20
          val filled = (smoothedProgress * barLength).toInt
          val bar = "[" + "=" * filled + " " * (barLength - filled) + "]"
          val percent = (smoothedProgress * 100).formatted("%.1f")

          // 输出进度信息
          print(s"\rProgress: $bar $percent% | Elapsed: $elapsedStr | ETA: $etaStr")

          // 更新状态
          lastProgress = smoothedProgress
          lastUpdateTime = currentTime
          Thread.sleep(1000)
        }
        println("\nAll tasks completed!")
      }

      // 辅助方法：格式化时间为 HH:MM:SS
      private def formatTime(seconds: Int): String = {
        val hours = seconds / 3600
        val minutes = (seconds % 3600) / 60
        val secs = seconds % 60
        f"$hours%02d:$minutes%02d:$secs%02d"
      }
    }

    new Thread(runnable).start()
  }
}
