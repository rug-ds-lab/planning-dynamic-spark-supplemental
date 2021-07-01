package rugds.experiments.planningbenefit.complex

import rugds.experiments.PlanningBenefitBaseDynamic
import rugds.experiments.scenarios.complex._

object PlanningBenefitComplexDynamic extends PlanningBenefitBaseDynamic {
  def main(args: Array[String]) {
    disableSparkLogs()

    val rawPatients = ScenarioComplex.makePatients(ScenarioComplex.PATIENT_COPIES * getDbSizeMultiplier)
    val heartRate = ScenarioComplex.makeHeartRateDB(sc, rawPatients)
    val bloodPressure = ScenarioComplex.makeBloodPressureDB(sc, rawPatients)
    val movement = ScenarioComplex.makeMovementDB(sc, rawPatients)
    val bedPressure = ScenarioComplex.makeBedPressureDB(sc, rawPatients)

    val dynamic = new ComplexDynamic(sc, server, heartRate, bloodPressure, movement, bedPressure)

    initializePipeline(dynamic)
    performIterations(dynamic, getExperimentIterations)

    shutdown()
  }
}
