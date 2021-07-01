package rugds.experiments.planningbenefit.simple

import rugds.experiments.PlanningBenefitBaseDynamic
import rugds.experiments.scenarios.simple._

object PlanningBenefitSimpleDynamic extends PlanningBenefitBaseDynamic {
  def main(args: Array[String]) {
    disableSparkLogs()

    val buildings = ScenarioSimple.makeBuildingDB(sc, ScenarioSimple.BUILDING_COPIES * getDbSizeMultiplier)

    val dynamic = new SimpleDynamic(sc, server, buildings)

    initializePipeline(dynamic)
    performIterations(dynamic, getExperimentIterations)

    shutdown()
  }
}
