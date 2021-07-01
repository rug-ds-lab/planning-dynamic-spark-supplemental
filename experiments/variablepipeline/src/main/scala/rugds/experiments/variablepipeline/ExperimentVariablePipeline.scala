package rugds.experiments.variablepipeline

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.dynamic.planning.{DAGExtractorListener, DynamicPlanner, PlanningVariationPoint}
import org.apache.spark.dynamic.planning.PlanningRDDFunctions._
import org.apache.spark.dynamic.planning.modules.OneToOnePlanningModule
import org.apache.spark.dynamic.planning.planner.ConstraintPlanner.JoinConstraintResolution
import org.apache.spark.dynamic.planning.planner.ConstraintPlanner.JoinConstraintResolution.JoinConstraintResolution
import org.apache.spark.dynamic.planning.rest.RESTOneToOnePlanningVariationPoint
import org.apache.spark.dynamic.planning.variables.VariableAssignment._
import org.apache.spark.dynamic.rest.DynamicServer
import org.apache.spark.rdd.RDD
import rugds.experiments.ExperimentBase

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class VariableModule(step: Int, alternative: Int) extends OneToOnePlanningModule[Int, Int](
    Seq("Step" =:= step),
    Seq("Step" := step + 1)
  ) {

  override def getName: String = super.getName + s"($step) alternative $alternative"

  override def applyTyped(in: Int): Int = {
    in * 2
  }
}

object ExperimentVariablePipeline extends ExperimentBase {
  final val EXPERIMENT_LENGTH = "length"
  final val EXPERIMENT_ALTERNATIVES = "alternatives"
  final val EXPERIMENT_JOIN_EITHER = "join-either"
  final val EXPERIMENT_JOIN_ALLEQUAL = "join-allequal"

  disableSparkLogs()

  val conf: SparkConf = makeConf()
  val sc: SparkContext = makeContext(conf)

  val server: DynamicServer = makeServer(conf)
  server.start()

  val planner: DynamicPlanner = makePlanner(server)
  val listener: DAGExtractorListener = new DAGExtractorListener(planner)

  sc.addSparkListener(listener)

  def main(args: Array[String]) {
    implicit val dynamicPlanner: DynamicPlanner = planner

    planner.removeJobOnJobEnd = false

    val steps = getStepCount
    val alternatives = getAlternativeCount
    val joins = getJoinCount

    val totalSteps = if (joins > 0) { steps * joins } else { steps }

    for (step <- 0 until totalSteps) {
      for (alternative <- 0 until alternatives) {
        planner.registerModule(new VariableModule(step, alternative))
      }
    }

    val rdd = sc
      .parallelize(Seq(1), 1)
      .initialConditions("Step" =:= 0)
      .asInstanceOf[RDD[Any]]

    val pipeline = if (joins > 0) {
      makeJoins(rdd, joins, steps)
    }
    else {
      makePipeline(rdd, steps)
    }

    val end = pipeline._1.goalConditions("Step" := totalSteps)
    end.plannedCollect[Int]()

    val jobId = planner.getLastJobId

    val results = performExperiment(jobId.get)

    val variableUnderTest = getTestName match {
      case EXPERIMENT_LENGTH => getStepCount
      case EXPERIMENT_ALTERNATIVES => getAlternativeCount
      case EXPERIMENT_JOIN_EITHER => getJoinCount
      case EXPERIMENT_JOIN_ALLEQUAL => getJoinCount
    }

    val timingMap = Map(variableUnderTest.toString -> results)
    writeTimings(timingMap, conf)
    showTimings(timingMap)

    server.stop()
    sc.stop()
  }

  override def getExperimentName: String = {
    s"${super.getExperimentName}-$getTestName"
  }

  def performExperiment(jobId: Int): mutable.ListBuffer[Long] = {
    val iterations = getExperimentIterations
    val timings = mutable.ListBuffer[Long]()
    for (itt <- 0 until iterations) {
      val t0 = System.nanoTime()
      Await.result(planner.replanPipeline(jobId), 2 minutes)
      val t1 = System.nanoTime()
      timings += (t1 - t0)
      info(planner.getJobData(jobId).get.plan.get.actions)
    }
    timings
  }

  def makeJoins(
    parent: RDD[Any], joins: Int, size: Int, variationPoints: Seq[PlanningVariationPoint] = Seq.empty
  )(implicit planner:DynamicPlanner): (RDD[Any], Seq[PlanningVariationPoint]) = {
    if (joins > 0) {
      val left = makePipeline(parent, size, variationPoints)
      val right = makePipeline(parent, size, variationPoints)

      val join = left._1.plannedUnion(right._1, getJoinType)
      val vps = (left._2 ++ right._2).distinct

      makeJoins(join, joins - 1, size, vps)
    }
    else {
      (parent, variationPoints)
    }
  }

  def makePipeline(
    parent: RDD[Any], size: Int, variationPoints: Seq[PlanningVariationPoint] = Seq.empty
  ): (RDD[Any], Seq[PlanningVariationPoint]) = {
    if (size > 0) {
      val vp: RESTOneToOnePlanningVariationPoint = new RESTOneToOnePlanningVariationPoint(server, planner, null)
      makePipeline(parent.plannedMap(f => vp(f), vp), size - 1, variationPoints :+ vp)
    }
    else {
      (parent, variationPoints)
    }
  }

  def getTestName: String = {
    val test = conf.get("spark.dynamic.experiments.test")
    assert(Seq(EXPERIMENT_LENGTH, EXPERIMENT_ALTERNATIVES, EXPERIMENT_JOIN_EITHER, EXPERIMENT_JOIN_ALLEQUAL).contains(test))
    test
  }

  def getStepCount: Int = {
    conf.get("spark.dynamic.experiments.steps").toInt
  }

  def getAlternativeCount: Int = {
    conf.get("spark.dynamic.experiments.alternatives").toInt
  }

  def getJoinCount: Int = {
    conf.get("spark.dynamic.experiments.joins").toInt
  }

  def getJoinType: JoinConstraintResolution = {
    getTestName match {
      case EXPERIMENT_JOIN_EITHER => JoinConstraintResolution.Either
      case EXPERIMENT_JOIN_ALLEQUAL => JoinConstraintResolution.AllEqual
    }
  }

  def getExperimentIterations: Int = {
    conf.get("spark.dynamic.experiments.iterations").toInt
  }
}
