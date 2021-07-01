package rugds.experiments.noplanoverhead.simple

import rugds.experiments.NoPlanOverheadBase
import rugds.experiments.scenarios.simple._

object NoPlanOverheadSimple extends NoPlanOverheadBase {
  def main(args: Array[String]) {
    disableSparkLogs()

    val buildings = ScenarioSimple.makeBuildingDB(sc, ScenarioSimple.BUILDING_COPIES * getDbSizeMultiplier)

    val planned = new SimplePlanned(sc, server, buildings)
    val dynamic = new SimpleDynamic(sc, server, buildings)
    val static = new SimpleStatic(sc, buildings)

    initializePipelines(planned, dynamic, static)
    preplanPipelines(planned, dynamic, static)
    performWarmup(planned, dynamic, static)
    performIterations(planned, dynamic, static)

    shutdown()
  }
}
