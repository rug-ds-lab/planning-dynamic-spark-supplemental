package rugds.experiments.scenarios.complex

import grizzled.slf4j.Logging
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import rugds.experiments.StaticPipeline

//object DynamicFuncs {
//  val filterAllSevere = (patient: PatientData, assessments: Seq[RiskAssessment]) => {
//    assessments.exists(_.risk > 100)
//  }
//
//  val filterAllHigh = (patient: PatientData, assessments: Seq[RiskAssessment]) => {
//    assessments.exists(_.risk > 50)
//  }
//
//  val filterAllLow = (patient: PatientData, assessments: Seq[RiskAssessment]) => {
//    assessments.exists(_.risk <= 50)
//  }
//
//  val notifyAllSevere = (patient: PatientData, assessments: Seq[RiskAssessment]) => {
//    (patient, FakeSMS.send(
//      s"Patient ${patient.patientId} (age ${patient.age})" +
//        s" has a the following risks: \n" +
//        assessments.map { a => s"[${a.data.getClass.getSimpleName}] -> ${a.risk}%"}.mkString("\n")
//    ))
//  }
//
//  val notifyAllHigh = (patient: PatientData, assessments: Seq[RiskAssessment]) => {
//    (patient, FakeEmail.send(
//      s"Patient ${patient.patientId} (age ${patient.age})" +
//        s" has a the following risks: \n" +
//        assessments.map { a => s"[${a.data.getClass.getSimpleName}] -> ${a.risk}%"}.mkString("\n")
//    ))
//  }
//
//  val notifyAllLow = (patient: PatientData, assessments: Seq[RiskAssessment]) => {
//    (patient, true)
//  }
//}

class ComplexStatic(
  sc: SparkContext,
  heartRate: RDD[(PatientData, HeartRate)],
  bloodPressure: RDD[(PatientData, BloodPressure)],
  movement: RDD[(PatientData, Movement)],
  bedPressure: RDD[(PatientData, BedPressure)]
) extends StaticPipeline {
  override def pipeline(): Int = {
    val hrMovementBedPressure = heartRate.join(movement).join(bedPressure).map(f => (f._1, (f._2._1._1, f._2._1._2, f._2._2)))

    val hr = hrMovementBedPressure.map { f =>
      // https://en.wikipedia.org/wiki/Heart_rate
      val maxHR = 220
      val minHR = 25

      val risk = if (f._2._1.v > maxHR || f._2._1.v < minHR) 100 else 0

      (f._1, RiskAssessment(f._2._1, risk))
    }
//      .map(f => (f._1, f._2))

    val hrBed = hrMovementBedPressure.map { f =>
      // https://en.wikipedia.org/wiki/Heart_rate
      val minSleep = 40
      val minRest = 50

      val sleeping = f._2._3.v > 1
      val minHR = if (sleeping) minSleep else minRest

      val risk = if (f._2._1.v < minHR) (minHR - f._2._1.v) * 20 else 0

      (f._1, RiskAssessment(f._2._3, risk))
    }
//      .map(f => (f._1, f._2))

    val hrMov = hrMovementBedPressure.map { f =>
      // https://en.wikipedia.org/wiki/Heart_rate
      val maxMoving = 211 - (0.64 * f._1.age) + 10.8
      val maxRest = 90

      val moving = f._2._2.v > 2
      val maxHR = if (moving) maxMoving else maxRest

      val risk = if (f._2._1.v > maxHR) (f._2._1.v - maxHR) * 20 else 0

      (f._1, RiskAssessment(f._2._2, risk))
    }
//      .map(f => (f._1, f._2))

    val bp = bloodPressure
      .map { f =>
        val acceptable = f._1.age match {
          // https://www.idealbloodpressureinfo.com/blood-pressure-chart-by-age/
          // BP depending on age                min        max
          case 1                           => (( 75, 50), (100, 75))
          case x if  1 until  6 contains x => (( 80, 55), (110, 79))
          case x if  6 until 14 contains x => (( 90, 60), (115, 80))
          case x if 14 until 20 contains x => ((105, 73), (120, 81))
          case x if 20 until 40 contains x => ((108, 75), (135, 86))
          //      case x if 40 until 65 contains x => ((112, 79), (147, 91))
          case _                           => ((112, 79), (147, 91))
        }

        val riskMin = f._2.systolic < acceptable._1._1 || f._2.diastolic < acceptable._1._2
        val riskMax = f._2.systolic > acceptable._2._1 || f._2.diastolic > acceptable._2._2

        val risk = (riskMin, riskMax) match {
          case (true, true) => 100
          case (true, _   ) => Math.max(acceptable._1._1 - f._2.systolic, acceptable._1._2 - f._2.diastolic) * 7
          case (_,    true) => Math.max(f._2.systolic - acceptable._1._1, f._2.diastolic - acceptable._1._2) * 7
          case (_,    _   ) => 0
        }

        (f._1, RiskAssessment(f._2, risk))
      }
//      .map(f => (f._1, f._2))

    val union1 = hr.union(hrBed)
//      .map(f => (f._1, f._2))
    val union2 = hrMov.union(bp)
//      .map(f => (f._1, f._2))

    val allProcessed = union1.union(union2)
//      .map(f => (f._1, f._2))

    val transformed = allProcessed.map(f => (f._1, f._2))

    val reduced = transformed.reduceByKey { (a: RiskAssessment, b: RiskAssessment) =>
      if (a.risk >= b.risk)
        a
      else
        b
    }

    val severe = reduced
      .filter(f => f._2.risk > 100)
      .map{ f =>
        (f._1, FakeSMS.send(
          s"Patient ${f._1.patientId} (age ${f._1.age})" +
            s" has a ${f._2.risk}% risk" +
            s" according to ${f._2.data.getClass.getSimpleName}"
        ))
      }
//      .map(f => (f._1, f._2))
    val high = reduced
      .filter(f => f._2.risk <= 100 && f._2.risk > 50)
      .map{ f =>
        (f._1, FakeEmail.send(
          s"Patient ${f._1.patientId} (age ${f._1.age})" +
            s" has a ${f._2.risk}% risk" +
            s" according to ${f._2.data.getClass.getSimpleName}"
        ))
      }
//      .map(f => (f._1, f._2))
    val low = reduced
      .filter(f => f._2.risk <= 50)
      .map{ f =>
        (f._1, true)
      }
//      .map(f => (f._1, f._2))

    val severehigh = severe.union(high)

    val allNotified = severehigh.union(low)
//      .map(f => (f._1, f._2))

    val result = allNotified.collect()

    result.length
  }

  def update(): Unit = ???
}

