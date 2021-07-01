package rugds.experiments.noplanoverhead.complex

import rugds.experiments.NoPlanOverheadBase
import rugds.experiments.scenarios.complex._

object NoPlanOverheadComplex extends NoPlanOverheadBase {
  def main(args: Array[String]) {
    disableSparkLogs()

    val rawPatients = ScenarioComplex.makePatients(ScenarioComplex.PATIENT_COPIES * getDbSizeMultiplier)
    val heartRate = ScenarioComplex.makeHeartRateDB(sc, rawPatients)
    val bloodPressure = ScenarioComplex.makeBloodPressureDB(sc, rawPatients)
    val movement = ScenarioComplex.makeMovementDB(sc, rawPatients)
    val bedPressure = ScenarioComplex.makeBedPressureDB(sc, rawPatients)

    val planned = new ComplexPlanned(sc, server, heartRate, bloodPressure, movement, bedPressure)
    val dynamic = new ComplexDynamic(sc, server, heartRate, bloodPressure, movement, bedPressure)
    val static = new ComplexStatic(sc, heartRate, bloodPressure, movement, bedPressure)

    initializePipelines(planned, dynamic, static)
    preplanPipelines(planned, dynamic, static)
    performWarmup(planned, dynamic, static)
    performIterations(planned, dynamic, static)

    shutdown()
  }
}
