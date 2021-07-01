package rugds.experiments

import java.io.{BufferedWriter, File, FileWriter}
import java.text.SimpleDateFormat
import java.util.Date

import grizzled.slf4j.Logging
import org.apache.spark.dynamic.planning.DynamicPlanner
import org.apache.spark.dynamic.planning.rest.RESTPlanner
import org.apache.spark.dynamic.rest.DynamicServer
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

class ExperimentBase extends Logging {
  final val DEFAULT_DB_SIZE_MULTIPLIER = 5

  def makeConf(): SparkConf = {
    new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .set("dynamic.server.port", "8090")
  }

  def makeContext(conf: SparkConf): SparkContext = {
    new SparkContext(conf)
  }

  def makeServer(conf: SparkConf): DynamicServer = {
    new DynamicServer(conf, conf.getInt("dynamic.server.port", 0))
  }

  def makePlanner(server: DynamicServer): DynamicPlanner = {
    new RESTPlanner(server)
  }

  def getDbSizeMultiplier(conf: SparkConf): Int = {
    conf.getInt("spark.dynamic.experiments.dbsize", DEFAULT_DB_SIZE_MULTIPLIER)
  }

  def disableSparkLogs() : Unit = {
    // To completely disable logging, you would:
    // org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.OFF)
    val logLevels = Map("WARN" -> Seq("org.apache"),
      "ERROR" -> Seq("org.spark-project", "org.eclipse", "org.apache.spark.dynamic.planning"),
      "INFO" -> Seq("org.apache.spark.dynamic"))
    // For more insight, you would instead set:
    // "ERROR" -> Seq("org.spark-project", "org.eclipse"),
    // "INFO" -> Seq("org.apache.spark.dynamic", "org.apache.spark.dynamic.planning.planner.encoders.ActionEncoder"),
    // "DEBUG" -> Seq("org.apache.spark.dynamic.planning"))
    logLevels.foreach { loggers =>
      loggers._2.foreach { pkg =>
        org.apache.log4j.Logger.getLogger(pkg).setLevel(org.apache.log4j.Level.toLevel(loggers._1))
      }
    }
  }

  def writeTimings(timings: Map[String, ListBuffer[Long]], conf: SparkConf): Unit = {
    val dir = conf.get("spark.dynamic.experiments.statisticsDir")
    timings.foreach { case (name: String, buff: ListBuffer[Long]) =>
      writeDurationToCSV(dir, s"${getExperimentName}_$name", buff)
    }
  }

  protected def getExperimentName: String = {
    this.getClass.getSimpleName
  }

  private def writeDurationToCSV(dir: String, name: String, buff: ListBuffer[Long]): Unit = {
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH'h'mmZ")
    val file =  new File(dir, s"${name}_${dateFormatter.format(new Date())}.csv")
    val writer = new BufferedWriter(new FileWriter(file))
    try {
      writer.write("duration\n")

      val count = math.max(buff.length, 1)
      var sum = 0L
      buff.foreach({ p =>
        sum += p
        writer.write(s"$p\n")
      })
      val avg =  sum / count

      writer.write(s";count:$count\n;sum:$sum\n;avg:$avg\n")
      writer.flush()
      info(s"${avg}ns (sum: ${sum}ns, count: $count) for $name")
    }
    finally {
      writer.close()
    }
  }

  def showTimings(timings: Map[String, ListBuffer[Long]]): Unit = {
    timings.foreach { case (exp, tms) =>
      info(s"$getExperimentName case: $exp\n  - Avg ${tms.sum / tms.length}.\n  - Min ${tms.min}.\n  - Max ${tms.max}")
    }
  }
}
