package rugds.experiments.scenarios.complex

import grizzled.slf4j.Logging
import org.apache.spark.SparkContext
import org.apache.spark.dynamic.DynamicVariationPoint
import org.apache.spark.dynamic.rest.{DynamicServer, RESTVariationPoint}
import org.apache.spark.dynamic.rdd.DynamicRDDFunctions._
import org.apache.spark.rdd.RDD
import rugds.experiments.DynamicPipeline

object DynamicFuncs {
  val processHeartRate = (patient: PatientData, data: (HeartRate, Movement, BedPressure)) => {
    // https://en.wikipedia.org/wiki/Heart_rate
    val maxHR = 220
    val minHR = 25

    val risk = if (data._1.v > maxHR || data._1.v < minHR) 100 else 0

    (patient, RiskAssessment(data._1, risk))
  }

  val processHeartRateSleeping = (patient: PatientData, data: (HeartRate, Movement, BedPressure)) => {
    // https://en.wikipedia.org/wiki/Heart_rate
    val minSleep = 40
    val minRest = 50

    val sleeping = data._3.v > 1
    val minHR = if (sleeping) minSleep else minRest

    val risk = if (data._1.v < minHR) (minHR - data._1.v) * 20 else 0

    (patient, RiskAssessment(data._3, risk))
  }

  val processHeartRateMoving = (patient: PatientData, data: (HeartRate, Movement, BedPressure)) => {
    // https://en.wikipedia.org/wiki/Heart_rate
    val maxMoving = 211 - (0.64 * patient.age) + 10.8
    val maxRest = 90

    val moving = data._2.v > 2
    val maxHR = if (moving) maxMoving else maxRest

    val risk = if (data._1.v > maxHR) (data._1.v - maxHR) * 20 else 0

    (patient, RiskAssessment(data._2, risk))
  }

  val processBloodPressure = (patient: PatientData, data: BloodPressure) => {
    val acceptable = patient.age match {
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

    val riskMin = data.systolic < acceptable._1._1 || data.diastolic < acceptable._1._2
    val riskMax = data.systolic > acceptable._2._1 || data.diastolic > acceptable._2._2

    val risk = (riskMin, riskMax) match {
      case (true, true) => 100
      case (true, _   ) => Math.max(acceptable._1._1 - data.systolic, acceptable._1._2 - data.diastolic) * 7
      case (_,    true) => Math.max(data.systolic - acceptable._1._1, data.diastolic - acceptable._1._2) * 7
      case (_,    _   ) => 0
    }

    (patient, RiskAssessment(data, risk))
  }

  val highestRisk = (a: RiskAssessment, b: RiskAssessment) => {
    if (a.risk >= b.risk)
      a
    else
      b
  }

  val filterSevere = (patient: PatientData, assessment: RiskAssessment) => {
    assessment.risk > 100
  }

  val filterHigh = (patient: PatientData, assessment: RiskAssessment) => {
    assessment.risk <= 100 && assessment.risk > 50
  }

  val filterLow = (patient: PatientData, assessment: RiskAssessment) => {
    assessment.risk <= 50
  }

  val notifySevere = (patient: PatientData, assessment: RiskAssessment) => {
    (patient, FakeSMS.send(
      s"Patient ${patient.patientId} (age ${patient.age})" +
        s" has a ${assessment.risk}% risk" +
        s" according to ${assessment.data.getClass.getSimpleName}"
    ))
  }

  val notifyHigh = (patient: PatientData, assessment: RiskAssessment) => {
    (patient, FakeEmail.send(
      s"Patient ${patient.patientId} (age ${patient.age})" +
        s" has a ${assessment.risk}% risk" +
        s" according to ${assessment.data.getClass.getSimpleName}"
    ))
  }

  val notifyLow = (patient: PatientData, assessment: RiskAssessment) => {
    (patient, true)
  }

  val noOpRiskAssessment = (patient: PatientData, assessment: RiskAssessment) => {
    (patient, assessment)
  }

  val noOpBoolean = (patient: PatientData, notified: Boolean) => {
    (patient, notified)
  }

  val allRisks = (a: Seq[RiskAssessment], b: Seq[RiskAssessment]) => {
    a ++ b
  }

  val riskAssessmentToSeq = (patient: PatientData, assessment: RiskAssessment) => {
    (patient, Seq(assessment))
  }

  val filterAllSevere = (patient: PatientData, assessments: Seq[RiskAssessment]) => {
    assessments.exists(_.risk > 100)
  }

  val filterAllHigh = (patient: PatientData, assessments: Seq[RiskAssessment]) => {
    assessments.exists(_.risk > 50)
  }

  val filterAllLow = (patient: PatientData, assessments: Seq[RiskAssessment]) => {
    assessments.exists(_.risk <= 50)
  }

  val notifyAllSevere = (patient: PatientData, assessments: Seq[RiskAssessment]) => {
    (patient, FakeSMS.send(
      s"Patient ${patient.patientId} (age ${patient.age})" +
        s" has a the following risks: \n" +
        assessments.map { a => s"[${a.data.getClass.getSimpleName}] -> ${a.risk}%"}.mkString("\n")
    ))
  }

  val notifyAllHigh = (patient: PatientData, assessments: Seq[RiskAssessment]) => {
    (patient, FakeEmail.send(
      s"Patient ${patient.patientId} (age ${patient.age})" +
        s" has a the following risks: \n" +
        assessments.map { a => s"[${a.data.getClass.getSimpleName}] -> ${a.risk}%"}.mkString("\n")
    ))
  }

  val notifyAllLow = (patient: PatientData, assessments: Seq[RiskAssessment]) => {
    (patient, true)
  }
}

class ComplexDynamic(
  sc: SparkContext,
  server: DynamicServer,
  heartRate: RDD[(PatientData, HeartRate)],
  bloodPressure: RDD[(PatientData, BloodPressure)],
  movement: RDD[(PatientData, Movement)],
  bedPressure: RDD[(PatientData, BedPressure)]
) extends DynamicPipeline {
  var processHR:         DynamicVariationPoint[(PatientData, (HeartRate, Movement, BedPressure)) => (PatientData, RiskAssessment)] = _
  var processHRBed:      DynamicVariationPoint[(PatientData, (HeartRate, Movement, BedPressure)) => (PatientData, RiskAssessment)] = _
  var processHRMov:      DynamicVariationPoint[(PatientData, (HeartRate, Movement, BedPressure)) => (PatientData, RiskAssessment)] = _
  var processBP:         DynamicVariationPoint[(PatientData, BloodPressure) => (PatientData, RiskAssessment)]                      = _

  var possibleTransform: DynamicVariationPoint[(PatientData, RiskAssessment) => (PatientData, RiskAssessment)] = _
  var reduceAssessments: DynamicVariationPoint[(RiskAssessment, RiskAssessment) => RiskAssessment]             = _
  var filterSevere:      DynamicVariationPoint[(PatientData, RiskAssessment) => Boolean]                       = _
  var filterHigh:        DynamicVariationPoint[(PatientData, RiskAssessment) => Boolean]                       = _
  var filterLow:         DynamicVariationPoint[(PatientData, RiskAssessment) => Boolean]                       = _
  var notifySevere:      DynamicVariationPoint[(PatientData, RiskAssessment) => (PatientData, Boolean)]        = _
  var notifyHigh:        DynamicVariationPoint[(PatientData, RiskAssessment) => (PatientData, Boolean)]        = _
  var notifyLow:         DynamicVariationPoint[(PatientData, RiskAssessment) => (PatientData, Boolean)]        = _

  var noOp1:  DynamicVariationPoint[(PatientData, RiskAssessment) => (PatientData, RiskAssessment)] = _
  var noOp2:  DynamicVariationPoint[(PatientData, RiskAssessment) => (PatientData, RiskAssessment)] = _
  var noOp3:  DynamicVariationPoint[(PatientData, RiskAssessment) => (PatientData, RiskAssessment)] = _
  var noOp4:  DynamicVariationPoint[(PatientData, RiskAssessment) => (PatientData, RiskAssessment)] = _
  var noOp5:  DynamicVariationPoint[(PatientData, RiskAssessment) => (PatientData, RiskAssessment)] = _
  var noOp6:  DynamicVariationPoint[(PatientData, RiskAssessment) => (PatientData, RiskAssessment)] = _
  var noOp7:  DynamicVariationPoint[(PatientData, RiskAssessment) => (PatientData, RiskAssessment)] = _
  var noOp8:  DynamicVariationPoint[(PatientData, Boolean) => (PatientData, Boolean)] = _
  var noOp9:  DynamicVariationPoint[(PatientData, Boolean) => (PatientData, Boolean)] = _
  var noOp10: DynamicVariationPoint[(PatientData, Boolean) => (PatientData, Boolean)] = _
  var noOp11: DynamicVariationPoint[(PatientData, Boolean) => (PatientData, Boolean)] = _

  override def initializeVariationPoints(): Unit = {
    processHR    = new RESTVariationPoint(server, "processHR",    0, DynamicFuncs.processHeartRate)
    processHRBed = new RESTVariationPoint(server, "processHRBed", 0, DynamicFuncs.processHeartRateSleeping)
    processHRMov = new RESTVariationPoint(server, "processHRMov", 0, DynamicFuncs.processHeartRateMoving)
    processBP    = new RESTVariationPoint(server, "processBP",    0, DynamicFuncs.processBloodPressure)

    possibleTransform = new RESTVariationPoint(server, "possibleTransform", 0, DynamicFuncs.noOpRiskAssessment)
    reduceAssessments = new RESTVariationPoint(server, "reduceAssessments", 0, DynamicFuncs.highestRisk)
    filterSevere      = new RESTVariationPoint(server, "filterSevere",      0, DynamicFuncs.filterSevere)
    filterHigh        = new RESTVariationPoint(server, "filterHigh",        0, DynamicFuncs.filterHigh)
    filterLow         = new RESTVariationPoint(server, "filterLow",         0, DynamicFuncs.filterLow)
    notifySevere      = new RESTVariationPoint(server, "notifySevere",      0, DynamicFuncs.notifySevere)
    notifyHigh        = new RESTVariationPoint(server, "notifyHigh",        0, DynamicFuncs.notifyHigh)
    notifyLow         = new RESTVariationPoint(server, "notifyLow",         0, DynamicFuncs.notifyLow)

    noOp1  = new RESTVariationPoint(server, "noOp1",  0, DynamicFuncs.noOpRiskAssessment)
    noOp2  = new RESTVariationPoint(server, "noOp2",  0, DynamicFuncs.noOpRiskAssessment)
    noOp3  = new RESTVariationPoint(server, "noOp3",  0, DynamicFuncs.noOpRiskAssessment)
    noOp4  = new RESTVariationPoint(server, "noOp4",  0, DynamicFuncs.noOpRiskAssessment)
    noOp5  = new RESTVariationPoint(server, "noOp5",  0, DynamicFuncs.noOpRiskAssessment)
    noOp6  = new RESTVariationPoint(server, "noOp6",  0, DynamicFuncs.noOpRiskAssessment)
    noOp7  = new RESTVariationPoint(server, "noOp7",  0, DynamicFuncs.noOpRiskAssessment)
    noOp8  = new RESTVariationPoint(server, "noOp8",  0, DynamicFuncs.noOpBoolean)
    noOp9  = new RESTVariationPoint(server, "noOp9",  0, DynamicFuncs.noOpBoolean)
    noOp10 = new RESTVariationPoint(server, "noOp10", 0, DynamicFuncs.noOpBoolean)
    noOp11 = new RESTVariationPoint(server, "noOp11", 0, DynamicFuncs.noOpBoolean)
  }

  override def pipeline(): Int = {
    innerPipeline(
      processHR,
      processHRBed,
      processHRMov,
      processBP,
      possibleTransform,
      reduceAssessments,
      filterSevere,
      filterHigh,
      filterLow,
      notifySevere,
      notifyHigh,
      notifyLow,
      noOp1,
      noOp2,
      noOp3,
      noOp4,
      noOp5,
      noOp6,
      noOp7,
      noOp8,
      noOp9,
      noOp10,
      noOp11
    )
  }

  def innerPipeline(
    processHR:         DynamicVariationPoint[(PatientData, (HeartRate, Movement, BedPressure)) => (PatientData, RiskAssessment)],
    processHRBed:      DynamicVariationPoint[(PatientData, (HeartRate, Movement, BedPressure)) => (PatientData, RiskAssessment)],
    processHRMov:      DynamicVariationPoint[(PatientData, (HeartRate, Movement, BedPressure)) => (PatientData, RiskAssessment)],
    processBP:         DynamicVariationPoint[(PatientData, BloodPressure) => (PatientData, RiskAssessment)],
    possibleTransform: DynamicVariationPoint[(PatientData, RiskAssessment) => (PatientData, RiskAssessment)],
    reduceAssessments: DynamicVariationPoint[(RiskAssessment, RiskAssessment) => RiskAssessment],
    filterSevere:      DynamicVariationPoint[(PatientData, RiskAssessment) => Boolean],
    filterHigh:        DynamicVariationPoint[(PatientData, RiskAssessment) => Boolean],
    filterLow:         DynamicVariationPoint[(PatientData, RiskAssessment) => Boolean],
    notifySevere:      DynamicVariationPoint[(PatientData, RiskAssessment) => (PatientData, Boolean)],
    notifyHigh:        DynamicVariationPoint[(PatientData, RiskAssessment) => (PatientData, Boolean)],
    notifyLow:         DynamicVariationPoint[(PatientData, RiskAssessment) => (PatientData, Boolean)],
    noOp1:  DynamicVariationPoint[(PatientData, RiskAssessment) => (PatientData, RiskAssessment)],
    noOp2:  DynamicVariationPoint[(PatientData, RiskAssessment) => (PatientData, RiskAssessment)],
    noOp3:  DynamicVariationPoint[(PatientData, RiskAssessment) => (PatientData, RiskAssessment)],
    noOp4:  DynamicVariationPoint[(PatientData, RiskAssessment) => (PatientData, RiskAssessment)],
    noOp5:  DynamicVariationPoint[(PatientData, RiskAssessment) => (PatientData, RiskAssessment)],
    noOp6:  DynamicVariationPoint[(PatientData, RiskAssessment) => (PatientData, RiskAssessment)],
    noOp7:  DynamicVariationPoint[(PatientData, RiskAssessment) => (PatientData, RiskAssessment)],
    noOp8:  DynamicVariationPoint[(PatientData, Boolean) => (PatientData, Boolean)],
    noOp9:  DynamicVariationPoint[(PatientData, Boolean) => (PatientData, Boolean)],
    noOp10: DynamicVariationPoint[(PatientData, Boolean) => (PatientData, Boolean)],
    noOp11: DynamicVariationPoint[(PatientData, Boolean) => (PatientData, Boolean)]
  ): Int = {
    val hrMovementBedPressure = heartRate.join(movement).join(bedPressure).map(f => (f._1, (f._2._1._1, f._2._1._2, f._2._2)))

    val hr = hrMovementBedPressure.dynamicMap(f => processHR.value(f._1, f._2), processHR)
//      .dynamicMap(f => noOp1.value(f._1, f._2), noOp1)

    val hrBed = hrMovementBedPressure.dynamicMap(f => processHRBed.value(f._1, f._2), processHRBed)
//      .dynamicMap(f => noOp2.value(f._1, f._2), noOp2)

    val hrMov = hrMovementBedPressure.dynamicMap(f => processHRMov.value(f._1, f._2), processHRMov)
//      .dynamicMap(f => noOp3.value(f._1, f._2), noOp3)

    val bp = bloodPressure
      .dynamicMap(f => processBP.value(f._1, f._2), processBP)
//      .dynamicMap(f => noOp4.value(f._1, f._2), noOp4)

    val union1 = hr.union(hrBed)
//      .dynamicMap(f => noOp5.value(f._1, f._2), noOp5)
    val union2 = hrMov.union(bp)
//      .dynamicMap(f => noOp6.value(f._1, f._2), noOp6)

    val allProcessed = union1.union(union2)
//      .dynamicMap(f => noOp7.value(f._1, f._2), noOp7)

    val transformed = allProcessed.dynamicMap(f => possibleTransform.value(f._1, f._2), possibleTransform)
    val reduced = transformed.dynamicReduceByKey(
      (a: RiskAssessment, b: RiskAssessment) => reduceAssessments.value(a, b),
      reduceAssessments
    )

    val severe = reduced
      .dynamicFilter(f => filterSevere.value(f._1, f._2), filterSevere)
      .dynamicMap(f => notifySevere.value(f._1, f._2), notifySevere)
//      .dynamicMap(f => noOp8.value(f._1, f._2), noOp8)
    val high = reduced
      .dynamicFilter(f => filterHigh.value(f._1, f._2), filterHigh)
      .dynamicMap(f => notifyHigh.value(f._1, f._2), notifyHigh)
//      .dynamicMap(f => noOp9.value(f._1, f._2), noOp9)
    val low = reduced
      .dynamicFilter(f => filterLow.value(f._1, f._2), filterLow)
      .dynamicMap(f => notifyLow.value(f._1, f._2), notifyLow)
//      .dynamicMap(f => noOp10.value(f._1, f._2), noOp10)

    val severehigh = severe.union(high)

    val allNotified = severehigh.union(low)
//      .dynamicMap(f => noOp11.value(f._1, f._2), noOp11)

    val result = allNotified.collect()

    result.length
  }

  def update(): Unit = ???
  // Not possible because the types are different between RiskAssessment and Seq[RiskAssessment]
//  {
//    possibleTransform.write(DynamicFuncs.riskAssessmentToSeq)
//    reduceAssessments.write(DynamicFuncs.allRisks)
//    filterSevere.write(DynamicFuncs.filterAllSevere)
//    filterHigh.write(DynamicFuncs.filterAllHigh)
//    filterLow.write(DynamicFuncs.filterAllLow)
//    notifySevere.write(DynamicFuncs.notifyAllSevere)
//    notifyHigh.write(DynamicFuncs.notifyAllHigh)
//    notifyLow.write(DynamicFuncs.notifyAllLow)
//  }
}