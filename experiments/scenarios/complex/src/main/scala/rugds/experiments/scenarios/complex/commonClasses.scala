package rugds.experiments.scenarios.complex

import scala.util.Random

trait DataType

case class PatientData(patientId: Long, age: Int) extends DataType
case class HeartRate(v: Double) extends DataType
case class BloodPressure(systolic: Double, diastolic: Double) extends DataType
case class Movement(v: Double) extends DataType
case class BedPressure(v: Double) extends DataType

case class RiskAssessment(data: DataType, risk: Double) extends DataType

object FakeSMS {
  def send(message: String): Boolean = {
    Random.nextInt(10) > 0
  }
}

object FakeEmail {
  def send(message: String): Boolean = {
    Random.nextInt(100) > 0
  }
}
