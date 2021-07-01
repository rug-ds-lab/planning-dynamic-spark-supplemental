package rugds.experiments

import org.apache.spark.dynamic.planning.{DAGExtractorListener, DynamicPlanner}
import org.apache.spark.dynamic.rest.DynamicServer
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

class NoPlanOverheadBase extends ExperimentBase {
  final val WARMUP_ITERATIONS = 20
  final val ITERATIONS = 60

  val conf: SparkConf = makeConf()
  val sc: SparkContext = makeContext(conf)

  val server: DynamicServer = makeServer(conf)
  server.start()

  val planner: DynamicPlanner = makePlanner(server)
  val listener: DAGExtractorListener = new DAGExtractorListener(planner)

  def shutdown(): Unit = {
    server.stop()
    sc.stop()
  }

  def getDbSizeMultiplier: Int = {
    getDbSizeMultiplier(conf)
  }

  def initializePipelines(planned: PlannedPipeline, dynamic: DynamicPipeline, static: StaticPipeline): Unit = {
    planner.registerModules(planned.defaultModules: _*)
    planned.initializeVariationPoints(planner)
    dynamic.initializeVariationPoints()
  }

  def preplanPipelines(planned: PlannedPipeline, dynamic: DynamicPipeline, static: StaticPipeline): Unit = {
    info(s"Pre-initializing plan for pipelines")
    info("== Planned ==")
    sc.addSparkListener(listener)
    planned.pipeline(planner)
    listener._debugRemoveFromSparkContext(sc)

    info("== Dynamic ==")
    dynamic.pipeline()

    info("== Static ==")
    static.pipeline()
  }

  def performWarmup(planned: PlannedPipeline, dynamic: DynamicPipeline, static: StaticPipeline): Unit = {
    info(s"Performing $WARMUP_ITERATIONS warmup iterations")
    for (itt <- 0 until WARMUP_ITERATIONS) {
      info(s"Iteration $itt")
      info("== Planned ==")
      planned.pipeline(planner)

      info("== Dynamic ==")
      dynamic.pipeline()

      info("== Static ==")
      static.pipeline()
    }
  }

  def performIterations(planned: PlannedPipeline, dynamic: DynamicPipeline, static: StaticPipeline): Unit = {
    val timings = Map(
      "planned" -> mutable.ListBuffer[Long](),
      "dynamic" -> mutable.ListBuffer[Long](),
      "static" -> mutable.ListBuffer[Long]()
    )

    info(s"Performing $ITERATIONS iterations")
    for( itt <- 0 until ITERATIONS ) {
      info(s"Iteration $itt")
      info("== Planned ==")
      // To include planning time, you would:
      // sc.addSparkListener(listener)
      val pt0 = System.nanoTime()
      planned.pipeline(planner)
      val pt1 = System.nanoTime()
      // listener._debugRemoveFromSparkContext(sc)

      info("== Dynamic ==")
      val dt0 = System.nanoTime()
      dynamic.pipeline()
      val dt1 = System.nanoTime()

      info("== Static ==")
      val st0 = System.nanoTime()
      static.pipeline()
      val st1 = System.nanoTime()

      timings("planned") += (pt1 - pt0)
      timings("dynamic") += (dt1 - dt0)
      timings("static")  += (st1 - st0)
    }
    writeTimings(timings, conf)
    showTimings(timings)
  }
}
