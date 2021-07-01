package rugds.experiments.noplanoverhead.middle

import rugds.experiments.NoPlanOverheadBase
import rugds.experiments.scenarios.middle._

object NoPlanOverheadMiddle extends NoPlanOverheadBase {
  def main(args: Array[String]) {
    disableSparkLogs()

    val vehicles = ScenarioMiddle.makeVehicleDB(sc, ScenarioMiddle.applyMultiplier(ScenarioMiddle.VEHICLE_COPIES, getDbSizeMultiplier))
    val accidents = ScenarioMiddle.makeAccidentDB(sc, ScenarioMiddle.applyMultiplier(ScenarioMiddle.ACCIDENT_COPIES, getDbSizeMultiplier))

    val planned = new MiddlePlanned(sc, server, vehicles, accidents)
    val dynamic = new MiddleDynamic(sc, server, vehicles, accidents)
    val static = new MiddleStatic(sc, vehicles, accidents)

    initializePipelines(planned, dynamic, static)
    preplanPipelines(planned, dynamic, static)
    performWarmup(planned, dynamic, static)
    performIterations(planned, dynamic, static)

    shutdown()
  }
}
