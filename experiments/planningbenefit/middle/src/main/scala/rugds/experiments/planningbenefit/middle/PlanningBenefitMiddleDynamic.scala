package rugds.experiments.planningbenefit.middle

import rugds.experiments.PlanningBenefitBaseDynamic
import rugds.experiments.scenarios.middle._

object PlanningBenefitMiddleDynamic extends PlanningBenefitBaseDynamic {
  def main(args: Array[String]) {
    disableSparkLogs()

    val vehicles = ScenarioMiddle.makeVehicleDB(sc, ScenarioMiddle.applyMultiplier(ScenarioMiddle.VEHICLE_COPIES, getDbSizeMultiplier))
    val accidents = ScenarioMiddle.makeAccidentDB(sc, ScenarioMiddle.applyMultiplier(ScenarioMiddle.ACCIDENT_COPIES, getDbSizeMultiplier))

    val dynamic = new MiddleDynamic(sc, server, vehicles, accidents)

    initializePipeline(dynamic)
    performIterations(dynamic, getExperimentIterations)

    shutdown()
  }
}
