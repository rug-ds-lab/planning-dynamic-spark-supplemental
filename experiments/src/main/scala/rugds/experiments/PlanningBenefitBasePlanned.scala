package rugds.experiments

import org.apache.spark.dynamic.planning.{DAGExtractorListener, DynamicPlanner}
import org.apache.spark.dynamic.rest.DynamicServer

class PlanningBenefitBasePlanned extends PlanningBenefitBase {
  val server: DynamicServer = makeServer(conf)
  server.start()

  val planner: DynamicPlanner = makePlanner(server)
  val listener: DAGExtractorListener = new DAGExtractorListener(planner)

  sc.addSparkListener(listener)

  override def shutdown(): Unit = {
    listener._debugRemoveFromSparkContext(sc)
    server.stop()
    super.shutdown()
  }

  def initializePipeline(planned: PlannedPipeline): Unit = {
    planner.registerModules(planned.defaultModules: _*)
    planned.initializeVariationPoints(planner)
  }

  def performIterations(planned: PlannedPipeline, iterations: Int): Unit = {
    for (itt <- 0 until iterations) {
      planned.pipeline(planner)

      // Clear old VPs
      planned.initializeVariationPoints(planner)
    }
  }
}
