package rugds.experiments.planningbenefit.middle

import rugds.experiments.PlanningBenefitBaseStatic
import rugds.experiments.scenarios.middle._

object PlanningBenefitMiddleStatic extends PlanningBenefitBaseStatic {
  def main(args: Array[String]) {
    disableSparkLogs()

    val vehicles = ScenarioMiddle.makeVehicleDB(sc, ScenarioMiddle.applyMultiplier(ScenarioMiddle.VEHICLE_COPIES, getDbSizeMultiplier))
    val accidents = ScenarioMiddle.makeAccidentDB(sc, ScenarioMiddle.applyMultiplier(ScenarioMiddle.ACCIDENT_COPIES, getDbSizeMultiplier))

    val static = new MiddleStatic(sc, vehicles, accidents)

    performIterations(static, getExperimentIterations)

    shutdown()
  }
}
