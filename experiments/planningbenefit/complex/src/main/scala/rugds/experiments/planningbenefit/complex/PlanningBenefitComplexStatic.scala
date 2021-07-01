package rugds.experiments.planningbenefit.complex

import rugds.experiments.PlanningBenefitBaseStatic
import rugds.experiments.scenarios.complex._

object PlanningBenefitComplexStatic extends PlanningBenefitBaseStatic {
  def main(args: Array[String]) {
    disableSparkLogs()

    val rawPatients = ScenarioComplex.makePatients(ScenarioComplex.PATIENT_COPIES * getDbSizeMultiplier)
    val heartRate = ScenarioComplex.makeHeartRateDB(sc, rawPatients)
    val bloodPressure = ScenarioComplex.makeBloodPressureDB(sc, rawPatients)
    val movement = ScenarioComplex.makeMovementDB(sc, rawPatients)
    val bedPressure = ScenarioComplex.makeBedPressureDB(sc, rawPatients)

    val static = new ComplexStatic(sc, heartRate, bloodPressure, movement, bedPressure)

    performIterations(static, getExperimentIterations)

    shutdown()
  }
}
