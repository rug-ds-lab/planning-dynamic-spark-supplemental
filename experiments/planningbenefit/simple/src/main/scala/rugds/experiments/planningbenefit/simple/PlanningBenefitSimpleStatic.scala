package rugds.experiments.planningbenefit.simple

import rugds.experiments.PlanningBenefitBaseStatic
import rugds.experiments.scenarios.simple._

object PlanningBenefitSimpleStatic extends PlanningBenefitBaseStatic {
  def main(args: Array[String]) {
    disableSparkLogs()

    val buildings = ScenarioSimple.makeBuildingDB(sc, ScenarioSimple.BUILDING_COPIES * getDbSizeMultiplier)

    val static = new SimpleStatic(sc, buildings)

    performIterations(static, getExperimentIterations)

    shutdown()
  }
}
