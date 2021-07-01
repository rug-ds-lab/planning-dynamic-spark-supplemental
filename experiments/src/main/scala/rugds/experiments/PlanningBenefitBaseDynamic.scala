package rugds.experiments

import org.apache.spark.dynamic.rest.DynamicServer

class PlanningBenefitBaseDynamic extends PlanningBenefitBase {
  val server: DynamicServer = makeServer(conf)
  server.start()

  override def shutdown(): Unit = {
    server.stop()
    super.shutdown()
  }

  def initializePipeline(dynamic: DynamicPipeline): Unit = {
    dynamic.initializeVariationPoints()
  }

  def performIterations(dynamic: DynamicPipeline, iterations: Int): Unit = {
    for (itt <- 0 until iterations) {
      dynamic.pipeline()
    }
  }
}
