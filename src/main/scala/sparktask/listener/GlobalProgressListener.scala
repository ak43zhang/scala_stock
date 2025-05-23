package sparktask.listener

import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession
import scala.collection.mutable
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration.Duration

/**
 * 全局进度条监听器
 */
class GlobalProgressListener extends SparkListener {
  private val stageTasks = mutable.HashMap[Int, (Int, Int)]()
  private val completedJobs = mutable.HashSet[Int]()
  private val completedStages = mutable.HashSet[Int]()
  private val submittedStages = mutable.HashSet[Int]()  // 新增：记录所有已提交的 Stage
  private val allDonePromise = Promise[Unit]()

  def globalProgress: Double = {
    val totalTasks = stageTasks.values.map(_._1).sum
    val completedTasks = stageTasks.values.map(_._2).sum
    if (totalTasks == 0) 0.0 else completedTasks.toDouble / totalTasks
  }

  def isAllDone: Boolean = allDonePromise.isCompleted

  def awaitCompletion(): Unit = {
    Await.ready(allDonePromise.future, Duration.Inf)
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    // 记录 Job 关联的所有 Stages
    jobStart.stageIds.foreach(submittedStages.add)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    completedJobs.add(jobEnd.jobId)
    checkAllDone()
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val stageId = stageSubmitted.stageInfo.stageId
    val numTasks = stageSubmitted.stageInfo.numTasks
    stageTasks(stageId) = (numTasks, 0)
    submittedStages.add(stageId)  // 记录已提交的 Stage
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageId = stageCompleted.stageInfo.stageId
    completedStages.add(stageId)
    checkAllDone()
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val stageId = taskEnd.stageId
    stageTasks.get(stageId).foreach { case (total, completed) =>
      stageTasks(stageId) = (total, completed + 1)
    }
  }

  // 新增方法：获取总 Task 数和已完成 Task 数
  def getTaskStats: (Int, Int) = {
    val total = stageTasks.values.map(_._1).sum
    val completed = stageTasks.values.map(_._2).sum
    (total, completed)
  }

  private def checkAllDone(): Unit = {
    // 关键修正：检查所有提交的 Stage 是否均完成
    val allStagesDone = submittedStages.forall(completedStages.contains)
    if (allStagesDone) {
      allDonePromise.trySuccess(())
    }
  }
}


