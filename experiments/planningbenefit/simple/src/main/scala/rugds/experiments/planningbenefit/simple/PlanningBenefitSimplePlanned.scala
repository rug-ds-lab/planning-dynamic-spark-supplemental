package rugds.experiments.planningbenefit.simple

import rugds.experiments.PlanningBenefitBasePlanned
import rugds.experiments.scenarios.simple._

object PlanningBenefitSimplePlanned extends PlanningBenefitBasePlanned {
  def main(args: Array[String]) {
    disableSparkLogs()

    val buildings = ScenarioSimple.makeBuildingDB(sc, ScenarioSimple.BUILDING_COPIES * getDbSizeMultiplier)

    val planned = new SimplePlanned(sc, server, buildings)

    initializePipeline(planned)
    performIterations(planned, getExperimentIterations)

    shutdown()
  }
}
