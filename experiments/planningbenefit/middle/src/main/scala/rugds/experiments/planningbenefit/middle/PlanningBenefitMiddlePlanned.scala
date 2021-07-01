package rugds.experiments.planningbenefit.middle

import rugds.experiments.PlanningBenefitBasePlanned
import rugds.experiments.scenarios.middle._

object PlanningBenefitMiddlePlanned extends PlanningBenefitBasePlanned {
  def main(args: Array[String]) {
    disableSparkLogs()

    val vehicles = ScenarioMiddle.makeVehicleDB(sc, ScenarioMiddle.applyMultiplier(ScenarioMiddle.VEHICLE_COPIES, getDbSizeMultiplier))
    val accidents = ScenarioMiddle.makeAccidentDB(sc, ScenarioMiddle.applyMultiplier(ScenarioMiddle.ACCIDENT_COPIES, getDbSizeMultiplier))

    val planned = new MiddlePlanned(sc, server, vehicles, accidents)

    initializePipeline(planned)
    performIterations(planned, getExperimentIterations)

    shutdown()
  }
}
