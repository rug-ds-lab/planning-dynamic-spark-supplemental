package rugds.experiments

class PlanningBenefitBaseStatic extends PlanningBenefitBase {
  def performIterations(static: StaticPipeline, iterations: Int): Unit = {
    for (itt <- 0 until iterations) {
      static.pipeline()
    }
  }
}
