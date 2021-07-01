package rugds.experiments.scenarios.complex

import grizzled.slf4j.Logging
import org.apache.spark.SparkContext
import org.apache.spark.dynamic.rest.DynamicServer
import org.apache.spark.dynamic.planning._
import org.apache.spark.dynamic.planning.rest._
import org.apache.spark.dynamic.planning.PlanningRDDFunctions._
import org.apache.spark.dynamic.planning.modules._
import org.apache.spark.dynamic.planning.planner.ConstraintPlanner.JoinConstraintResolution
import org.apache.spark.dynamic.planning.variationpoints._
import org.apache.spark.dynamic.planning.variables.VariableAssignment._
import org.apache.spark.rdd.RDD
import rugds.experiments.PlannedPipeline

import scala.collection.mutable

object ProcessHeartRate
  extends PairToPairPlanningModule[PatientData, (HeartRate, Movement, BedPressure), PatientData, RiskAssessment](
    effects = Seq("HeartRateProcessed" := true)
  ) {
  override def applyTyped(patient: PatientData, data: (HeartRate, Movement, BedPressure)): (PatientData, RiskAssessment) = {
    // https://en.wikipedia.org/wiki/Heart_rate
    val maxHR = 220
    val minHR = 25

    val risk = if (data._1.v > maxHR || data._1.v < minHR) 100 else 0

    (patient, RiskAssessment(data._1, risk))
  }
}

object ProcessHeartRateSleeping
  extends PairToPairPlanningModule[PatientData, (HeartRate, Movement, BedPressure), PatientData, RiskAssessment](
    effects = Seq("HeartRateSleepingProcessed" := true)
  ) {
  override def applyTyped(patient: PatientData, data: (HeartRate, Movement, BedPressure)): (PatientData, RiskAssessment) = {
    // https://en.wikipedia.org/wiki/Heart_rate
    val minSleep = 40
    val minRest = 50

    val sleeping = data._3.v > 1
    val minHR = if (sleeping) minSleep else minRest

    val risk = if (data._1.v < minHR) (minHR - data._1.v) * 20 else 0

    (patient, RiskAssessment(data._3, risk))
  }
}

object ProcessHeartRateMoving
  extends PairToPairPlanningModule[PatientData, (HeartRate, Movement, BedPressure), PatientData, RiskAssessment](
    effects = Seq("HeartRateMovingProcessed" := true)
  ) {
  override def applyTyped(patient: PatientData, data: (HeartRate, Movement, BedPressure)): (PatientData, RiskAssessment) = {
    // https://en.wikipedia.org/wiki/Heart_rate
    val maxMoving = 211 - (0.64 * patient.age) + 10.8
    val maxRest = 90

    val moving = data._2.v > 2
    val maxHR = if (moving) maxMoving else maxRest

    val risk = if (data._1.v > maxHR) (data._1.v - maxHR) * 20 else 0

    (patient, RiskAssessment(data._2, risk))
  }
}

object ProcessBloodPressure
  extends PairToPairPlanningModule[PatientData, BloodPressure, PatientData, RiskAssessment](
    effects = Seq("BloodPressureProcessed" := true)
  ) {
  override def applyTyped(patient: PatientData, data: BloodPressure): (PatientData, RiskAssessment) = {
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
}

object HighestRisk
  extends PairToOnePlanningModule[RiskAssessment, RiskAssessment, RiskAssessment] {
  override def applyTyped(a: RiskAssessment, b: RiskAssessment): RiskAssessment = {
    if (a.risk >= b.risk)
      a
    else
      b
  }
}

object FilterSevere
  extends PairToOnePlanningModule[PatientData, RiskAssessment, Boolean](
    effects = Seq("SevereFiltered" := true)
  ) {
  override def applyTyped(patient: PatientData, assessment: RiskAssessment): Boolean = {
    assessment.risk > 100
  }
}

object FilterHigh
  extends PairToOnePlanningModule[PatientData, RiskAssessment, Boolean](
    effects = Seq("HighFiltered" := true)
  ) {
  override def applyTyped(patient: PatientData, assessment: RiskAssessment): Boolean = {
    assessment.risk <= 100 && assessment.risk > 50
  }
}

object FilterLow
  extends PairToOnePlanningModule[PatientData, RiskAssessment, Boolean](
    effects = Seq("LowFiltered" := true)
  ) {
  override def applyTyped(patient: PatientData, assessment: RiskAssessment): Boolean = {
    assessment.risk <= 50
  }
}

object NotifySevere
  extends PairToPairPlanningModule[PatientData, RiskAssessment, PatientData, Boolean](
    Seq("SevereFiltered" =:= true),
    Seq("SevereNotified" := true)
  ) {
  override def applyTyped(patient: PatientData, assessment: RiskAssessment): (PatientData, Boolean) = {
    (patient, FakeSMS.send(
      s"Patient ${patient.patientId} (age ${patient.age})" +
      s" has a ${assessment.risk}% risk" +
      s" according to ${assessment.data.getClass.getSimpleName}"
    ))
  }
}

object NotifyHigh
  extends PairToPairPlanningModule[PatientData, RiskAssessment, PatientData, Boolean](
    Seq("HighFiltered" =:= true),
    Seq("HighNotified" := true)
  ) {
  override def applyTyped(patient: PatientData, assessment: RiskAssessment): (PatientData, Boolean) = {
    (patient, FakeEmail.send(
      s"Patient ${patient.patientId} (age ${patient.age})" +
        s" has a ${assessment.risk}% risk" +
        s" according to ${assessment.data.getClass.getSimpleName}"
    ))
  }
}

object NotifyLow
  extends PairToPairPlanningModule[PatientData, RiskAssessment, PatientData, Boolean](
    Seq("LowFiltered" =:= true),
    Seq("LowNotified" := true)
  ) {
  override def applyTyped(patient: PatientData, assessment: RiskAssessment): (PatientData, Boolean) = {
    (patient, true)
  }
}

object AllRisks
  extends PairToOnePlanningModule[Seq[RiskAssessment], Seq[RiskAssessment], Seq[RiskAssessment]] {
  override def applyTyped(a: Seq[RiskAssessment], b: Seq[RiskAssessment]): Seq[RiskAssessment] = {
    a ++ b
  }
}

object RiskAssessmentToSeq
  extends PairToPairPlanningModule[PatientData, RiskAssessment, PatientData, Seq[RiskAssessment]] {
  override def applyTyped(patient: PatientData, assessment: RiskAssessment): (PatientData, Seq[RiskAssessment]) = {
    (patient, Seq(assessment))
  }
}

object FilterAllSevere
  extends PairToOnePlanningModule[PatientData, Seq[RiskAssessment], Boolean](
    effects = Seq("SevereFiltered" := true)
  ) {
  override def applyTyped(patient: PatientData, assessments: Seq[RiskAssessment]): Boolean = {
    assessments.exists(_.risk > 100)
  }
}

object FilterAllHigh
  extends PairToOnePlanningModule[PatientData, Seq[RiskAssessment], Boolean](
    effects = Seq("HighFiltered" := true)
  ) {
  override def applyTyped(patient: PatientData, assessments: Seq[RiskAssessment]): Boolean = {
    assessments.exists(_.risk > 50)
  }
}

object FilterAllLow
  extends PairToOnePlanningModule[PatientData, Seq[RiskAssessment], Boolean](
    effects = Seq("LowFiltered" := true)
  ) {
  override def applyTyped(patient: PatientData, assessments: Seq[RiskAssessment]): Boolean = {
    assessments.exists(_.risk <= 50)
  }
}

object NotifyAllSevere
  extends PairToPairPlanningModule[PatientData, Seq[RiskAssessment], PatientData, Boolean](
    Seq("SevereFiltered" =:= true),
    Seq("SevereNotified" := true)
  ) {
  override def applyTyped(patient: PatientData, assessments: Seq[RiskAssessment]): (PatientData, Boolean) = {
    (patient, FakeSMS.send(
      s"Patient ${patient.patientId} (age ${patient.age})" +
        s" has a the following risks: \n" +
        assessments.map { a => s"[${a.data.getClass.getSimpleName}] -> ${a.risk}%"}.mkString("\n")
    ))
  }
}

object NotifyAllHigh
  extends PairToPairPlanningModule[PatientData, Seq[RiskAssessment], PatientData, Boolean](
    Seq("HighFiltered" =:= true),
    Seq("HighNotified" := true)
  ) {
  override def applyTyped(patient: PatientData, assessments: Seq[RiskAssessment]): (PatientData, Boolean) = {
    (patient, FakeEmail.send(
      s"Patient ${patient.patientId} (age ${patient.age})" +
        s" has a the following risks: \n" +
        assessments.map { a => s"[${a.data.getClass.getSimpleName}] -> ${a.risk}%"}.mkString("\n")
    ))
  }
}

object NotifyAllLow
  extends PairToPairPlanningModule[PatientData, Seq[RiskAssessment], PatientData, Boolean](
    Seq("LowFiltered" =:= true),
    Seq("LowNotified" := true)
  ) {
  override def applyTyped(patient: PatientData, assessments: Seq[RiskAssessment]): (PatientData, Boolean) = {
    (patient, true)
  }
}

class ComplexPlanned(
    sc: SparkContext,
    server: DynamicServer,
    heartRate: RDD[(PatientData, HeartRate)],
    bloodPressure: RDD[(PatientData, BloodPressure)],
    movement: RDD[(PatientData, Movement)],
    bedPressure: RDD[(PatientData, BedPressure)]
) extends PlannedPipeline {
  var processHR:    PairToPairPlanningVariationPoint = _
  var processHRBed: PairToPairPlanningVariationPoint = _
  var processHRMov: PairToPairPlanningVariationPoint = _
  var processBP:    PairToPairPlanningVariationPoint = _

  var possibleTransform: PairToPairPlanningVariationPoint = _
  var reduceAssessments:  PairToOnePlanningVariationPoint = _
  var filterSevere:       PairToOnePlanningVariationPoint = _
  var filterHigh:         PairToOnePlanningVariationPoint = _
  var filterLow:          PairToOnePlanningVariationPoint = _
  var notifySevere:      PairToPairPlanningVariationPoint = _
  var notifyHigh:        PairToPairPlanningVariationPoint = _
  var notifyLow:         PairToPairPlanningVariationPoint = _

  var noOp1:  PairToPairPlanningVariationPoint = _
  var noOp2:  PairToPairPlanningVariationPoint = _
  var noOp3:  PairToPairPlanningVariationPoint = _
  var noOp4:  PairToPairPlanningVariationPoint = _
  var noOp5:  PairToPairPlanningVariationPoint = _
  var noOp6:  PairToPairPlanningVariationPoint = _
  var noOp7:  PairToPairPlanningVariationPoint = _
  var noOp8:  PairToPairPlanningVariationPoint = _
  var noOp9:  PairToPairPlanningVariationPoint = _
  var noOp10: PairToPairPlanningVariationPoint = _
  var noOp11: PairToPairPlanningVariationPoint = _

  override def defaultModules: Seq[DynamicModule[_, _]] = ScenarioComplex.DEFAULT_MODULES

  override def initializeVariationPoints(planner: DynamicPlanner): Unit = {
    processHR    = new RESTPairToPairPlanningVariationPoint(server, planner, ProcessHeartRate)
    processHRBed = new RESTPairToPairPlanningVariationPoint(server, planner, ProcessHeartRateSleeping)
    processHRMov = new RESTPairToPairPlanningVariationPoint(server, planner, ProcessHeartRateMoving)
    processBP    = new RESTPairToPairPlanningVariationPoint(server, planner, ProcessBloodPressure)

    possibleTransform = new RESTPairToPairPlanningVariationPoint(server, planner, null)
    reduceAssessments = new RESTPairToOnePlanningVariationPoint(server, planner, HighestRisk)
    filterSevere      = new RESTPairToOnePlanningVariationPoint(server, planner, FilterSevere)
    filterHigh        = new RESTPairToOnePlanningVariationPoint(server, planner, FilterHigh)
    filterLow         = new RESTPairToOnePlanningVariationPoint(server, planner, FilterLow)
    notifySevere      = new RESTPairToPairPlanningVariationPoint(server, planner, NotifySevere)
    notifyHigh        = new RESTPairToPairPlanningVariationPoint(server, planner, NotifyHigh)
    notifyLow         = new RESTPairToPairPlanningVariationPoint(server, planner, NotifyLow)

    noOp1 = new RESTPairToPairPlanningVariationPoint(server, planner, null)
    noOp2 = new RESTPairToPairPlanningVariationPoint(server, planner, null)
    noOp3 = new RESTPairToPairPlanningVariationPoint(server, planner, null)
    noOp4 = new RESTPairToPairPlanningVariationPoint(server, planner, null)
    noOp5 = new RESTPairToPairPlanningVariationPoint(server, planner, null)
    noOp6 = new RESTPairToPairPlanningVariationPoint(server, planner, null)
    noOp7 = new RESTPairToPairPlanningVariationPoint(server, planner, null)
    noOp8 = new RESTPairToPairPlanningVariationPoint(server, planner, null)
    noOp9 = new RESTPairToPairPlanningVariationPoint(server, planner, null)
    noOp10 = new RESTPairToPairPlanningVariationPoint(server, planner, null)
    noOp11 = new RESTPairToPairPlanningVariationPoint(server, planner, null)
  }

  override def pipeline(planner: DynamicPlanner): Int = {
    innerPipeline(
      planner,
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
    planner: DynamicPlanner,
    processHR:    PairToPairPlanningVariationPoint,
    processHRBed: PairToPairPlanningVariationPoint,
    processHRMov: PairToPairPlanningVariationPoint,
    processBP:    PairToPairPlanningVariationPoint,
    possibleTransform: PairToPairPlanningVariationPoint,
    reduceAssessments:  PairToOnePlanningVariationPoint,
    filterSevere:       PairToOnePlanningVariationPoint,
    filterHigh:         PairToOnePlanningVariationPoint,
    filterLow:          PairToOnePlanningVariationPoint,
    notifySevere:      PairToPairPlanningVariationPoint,
    notifyHigh:        PairToPairPlanningVariationPoint,
    notifyLow:         PairToPairPlanningVariationPoint,
    noOp1:  PairToPairPlanningVariationPoint,
    noOp2:  PairToPairPlanningVariationPoint,
    noOp3:  PairToPairPlanningVariationPoint,
    noOp4:  PairToPairPlanningVariationPoint,
    noOp5:  PairToPairPlanningVariationPoint,
    noOp6:  PairToPairPlanningVariationPoint,
    noOp7:  PairToPairPlanningVariationPoint,
    noOp8:  PairToPairPlanningVariationPoint,
    noOp9:  PairToPairPlanningVariationPoint,
    noOp10: PairToPairPlanningVariationPoint,
    noOp11: PairToPairPlanningVariationPoint
  ): Int = {
    implicit val dynamicPlanner = planner
    val defaults = Seq(
      "HeartRateProcessed" =:= false,
      "HeartRateSleepingProcessed" =:= false,
      "HeartRateMovingProcessed" =:= false,
      "BloodPressureProcessed" =:= false,
      "SevereNotified" =:= false,
      "HighNotified" =:= false,
      "LowNotified" =:= false,
      "SevereFiltered" =:= false,
      "HighFiltered" =:= false,
      "LowFiltered" =:= false
    )
    val hrMovementBedPressure = heartRate.join(movement).join(bedPressure).map(f => (f._1, (f._2._1._1, f._2._1._2, f._2._2)))
      .initialConditions(defaults: _*)

    val hr = hrMovementBedPressure.plannedMap(f => processHR(f._1, f._2), processHR)
//      .plannedMap(f => noOp1(f._1, f._2), noOp1)

    val hrBed = hrMovementBedPressure.plannedMap(f => processHRBed(f._1, f._2), processHRBed)
//      .plannedMap(f => noOp2(f._1, f._2), noOp2)

    val hrMov = hrMovementBedPressure.plannedMap(f => processHRMov(f._1, f._2), processHRMov)
//      .plannedMap(f => noOp3(f._1, f._2), noOp3)

    val bp = bloodPressure
      .initialConditions(defaults: _*)
      .plannedMap(f => processBP(f._1, f._2), processBP)
//      .plannedMap(f => noOp4(f._1, f._2), noOp4)

    val union1 = hr.plannedUnion(hrBed, JoinConstraintResolution.Either)
//      .plannedMap(f => noOp5(f._1, f._2), noOp5)
    val union2 = hrMov.plannedUnion(bp, JoinConstraintResolution.Either)
//      .plannedMap(f => noOp6(f._1, f._2), noOp6)

    val allProcessed = union1.plannedUnion(union2, JoinConstraintResolution.Either)
//      .plannedMap(f => noOp7(f._1, f._2), noOp7)

    val transformed = allProcessed.plannedMap(f => possibleTransform(f._1, f._2), possibleTransform)
    val reduced = transformed.plannedReduceByKey((a: Any, b: Any) => reduceAssessments(a, b), reduceAssessments)

    val severe = reduced
      .plannedFilter(f => filterSevere(f._1, f._2).asInstanceOf[Boolean], filterSevere)
      .plannedMap(f => notifySevere(f._1, f._2), notifySevere)
//      .plannedMap(f => noOp8(f._1, f._2), noOp8)
    val high = reduced
      .plannedFilter(f => filterHigh(f._1, f._2).asInstanceOf[Boolean], filterHigh)
      .plannedMap(f => notifyHigh(f._1, f._2), notifyHigh)
//      .plannedMap(f => noOp9(f._1, f._2), noOp9)
    val low = reduced
      .plannedFilter(f => filterLow(f._1, f._2).asInstanceOf[Boolean], filterLow)
      .plannedMap(f => notifyLow(f._1, f._2), notifyLow)
//      .plannedMap(f => noOp10(f._1, f._2), noOp10)

    val severehigh = severe.plannedUnion(high, JoinConstraintResolution.Either)

    val allNotified = severehigh.plannedUnion(low, JoinConstraintResolution.Either)
//      .plannedMap(f => noOp11(f._1, f._2), noOp11)

    allNotified.goalConditions(
      "HeartRateProcessed" =:= true,
      "HeartRateSleepingProcessed" =:= true,
      "HeartRateMovingProcessed" =:= true,
      "BloodPressureProcessed" =:= true,
      "SevereNotified" =:= true,
      "HighNotified" =:= true,
      "LowNotified" =:= true
    )

    val result = allNotified.plannedCollect[(PatientData, Boolean)]()

    result.length
  }

  def update(planner: DynamicPlanner): Unit = {
    val jobId = planner.getLastJobId.get
    planner.registerModule(AllRisks)
    planner.removeModule(HighestRisk.getName)
    planner.replanPipeline(jobId)
  }
}