package rugds.experiments.scenarios.complex

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import scala.collection.mutable.ListBuffer

object ScenarioComplex {
  final val PATIENT_COPIES = 300

  final val DEFAULT_MODULES = Seq(
    ProcessHeartRate, ProcessHeartRateSleeping, ProcessHeartRateMoving, ProcessBloodPressure,
    HighestRisk, FilterSevere, FilterHigh, FilterLow, NotifySevere, NotifyHigh, NotifyLow,
    FilterAllSevere, FilterAllHigh, FilterAllLow, NotifyAllSevere, NotifyAllHigh, NotifyAllLow,
    RiskAssessmentToSeq
  )

  def makePatients(count: Int): List[PatientData] = {
    val buff = new ListBuffer[PatientData]()
    for( itt <- 0 until count ){
      buff ++= Seq(
        PatientData(itt * 10 + 0, 78),
        PatientData(itt * 10 + 1, 60),
        PatientData(itt * 10 + 2, 78),
        PatientData(itt * 10 + 3, 78),
        PatientData(itt * 10 + 4, 78),
        PatientData(itt * 10 + 5, 78),
        PatientData(itt * 10 + 6, 78),
        PatientData(itt * 10 + 7, 78),
        PatientData(itt * 10 + 8, 78),
        PatientData(itt * 10 + 9, 78),
        PatientData(itt * 10 + 10, 78)
      )
    }
    buff.toList
  }

  def makeHeartRateDB(sc: SparkContext, patientData: List[PatientData]): RDD[(PatientData, HeartRate)] = {
    val buff = patientData.map { patient =>
      (patient, HeartRate(patient.patientId % 80 + 30))
    }
    sc.parallelize(buff, Math.max(sc.defaultParallelism, Math.round(buff.size / 100)))
  }

  def makeBloodPressureDB(sc: SparkContext, patientData: List[PatientData]): RDD[(PatientData, BloodPressure)] = {
    val buff = patientData.map { patient =>
      (patient, BloodPressure(patient.patientId % 80 + 80, patient.patientId % 60 + 50))
    }
    sc.parallelize(buff, Math.max(sc.defaultParallelism, Math.round(buff.size / 100)))
  }

  def makeMovementDB(sc: SparkContext, patientData: List[PatientData]): RDD[(PatientData, Movement)] = {
    val buff = patientData.map { patient =>
      (patient, Movement(patient.patientId % 3))
    }
    sc.parallelize(buff, Math.max(sc.defaultParallelism, Math.round(buff.size / 100)))
  }

  def makeBedPressureDB(sc: SparkContext, patientData: List[PatientData]): RDD[(PatientData, BedPressure)] = {
    val buff = patientData.map { patient =>
      (patient, BedPressure(patient.patientId % 3))
    }
    sc.parallelize(buff, Math.max(sc.defaultParallelism, Math.round(buff.size / 100)))
  }
}
