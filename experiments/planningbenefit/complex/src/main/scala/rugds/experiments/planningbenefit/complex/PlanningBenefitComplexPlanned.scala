package rugds.experiments.planningbenefit.complex

import rugds.experiments.PlanningBenefitBasePlanned
import rugds.experiments.scenarios.complex._

object PlanningBenefitComplexPlanned extends PlanningBenefitBasePlanned {
  def main(args: Array[String]) {
    disableSparkLogs()

    val rawPatients = ScenarioComplex.makePatients(ScenarioComplex.PATIENT_COPIES * getDbSizeMultiplier)
    val heartRate = ScenarioComplex.makeHeartRateDB(sc, rawPatients)
    val bloodPressure = ScenarioComplex.makeBloodPressureDB(sc, rawPatients)
    val movement = ScenarioComplex.makeMovementDB(sc, rawPatients)
    val bedPressure = ScenarioComplex.makeBedPressureDB(sc, rawPatients)

    val planned = new ComplexPlanned(sc, server, heartRate, bloodPressure, movement, bedPressure)

    initializePipeline(planned)
    performIterations(planned, getExperimentIterations)

    shutdown()
  }
}
