package rugds.experiments.scenarios.middle

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

object DistanceToOther
  extends PairToPairPlanningModule[VehicleData,AccidentData, (VehicleData,AccidentData), Meters](
    effects = Seq("DistanceToOther" := true)
  ) {
  override def applyTyped(vehicle: VehicleData, accident: AccidentData): ((VehicleData,AccidentData), Meters) = {
    ((vehicle, accident), Meters(vehicle.locationOther.v - vehicle.locationNow.v))
  }
}

object NotAlreadyPassed extends PairToOnePlanningModule[VehicleData,AccidentData, Boolean] {
  override def applyTyped(vehicle: VehicleData, accident: AccidentData): Boolean = {
    accident.locationAccident.v >= vehicle.locationNow.v && vehicle.locationNow.v < vehicle.locationOther.v
  }
}

object DistanceToAccidentKM
  extends PairToPairPlanningModule[VehicleData,AccidentData, (VehicleData,AccidentData),Kilometers](
    effects = Seq("DistanceToAccident" := true)) {
  override def applyTyped(vehicle: VehicleData, accident: AccidentData): ((VehicleData,AccidentData),Kilometers) = {
    ((vehicle, accident), Kilometers((accident.locationAccident.v - vehicle.locationNow.v) / 1000))
  }
}

//object DistanceToAccidentM
//  extends PairToPairPlanningModule[VehicleData, AccidentData, (VehicleData, AccidentData), Meters](
//    effects = Seq("DistanceToAccident" := true)
//  ) {
//  override def applyTyped(vehicle: VehicleData, accident: AccidentData): ((VehicleData, AccidentData), Meters) = {
//    ((vehicle, accident), Meters(accident.locationAccident.v - vehicle.locationNow.v))
//  }
//}

object DistanceDesiredBasedOnAccident
  extends PairToPairPlanningModule[(VehicleData,AccidentData),Kilometers, (VehicleData,AccidentData),Kilometers](
    Seq("DistanceToAccident" =:= true),
    Seq("DistanceDesired" := true)
  ) {
  override def applyTyped(
    instance: (VehicleData, AccidentData),
    distance: Kilometers
  ): ((VehicleData,AccidentData),Kilometers) = {
    (instance,
      if      (distance.v > Kilometers(2).v)
        Kilometers(0.02)
      else if (distance.v > Kilometers(0.5).v)
        Kilometers(0.03)
      else
        Kilometers(0.035)
    )
  }
}

object DistanceDesiredBasedOnSpeed
  extends PairToPairPlanningModule[VehicleData,AccidentData, (VehicleData,AccidentData), Meters](
    effects = Seq("DistanceDesired" := true)
  ) {
  override def applyTyped(vehicle: VehicleData, accident: AccidentData): ((VehicleData,AccidentData), Meters) = {
    ((vehicle, accident), Meters(Math.max(100, (vehicle.speedNow.v + vehicle.speedNow.v/ 2) * 0.9)))
  }
}

//object DistanceDesiredV2
//  extends PairToPairPlanningModule[(VehicleData,AccidentData),Meters, (VehicleData,AccidentData),Meters](
//    Seq("DistanceToAccident" =:= true),
//    Seq("DistanceDesired" := true)
//  ) {
//  override def applyTyped(
//    instance: (VehicleData, AccidentData),
//    distance: Meters
//  ): ((VehicleData,AccidentData),Meters) = {
//    (instance,
//      if      (distance.v > Meters(2000).v && instance._1.speedNow.v > KMpH(50).v)
//        Meters(20)
//      else if (distance.v > Meters(2000).v)
//        Meters(15)
//      else
//        Meters(instance._1.speedNow.toMpS.v * 2)
//    )
//  }
//}

object KilometersToMeters
  extends PairToPairPlanningModule[(VehicleData,AccidentData),Kilometers, (VehicleData,AccidentData),Meters] {
  override def applyTyped(
    instance: (VehicleData, AccidentData),
    distance: Kilometers
  ): ((VehicleData,AccidentData),Meters) = {
    (instance, Meters(distance.v * 1000))
  }
}

object SpeedDesired
  extends PairToPairPlanningModule[(VehicleData,AccidentData),(Meters,Meters), (VehicleData,AccidentData),(MpS,Meters,Meters)](
    Seq("DistanceDesired" =:= true, "DistanceToOther" =:= true),
    Seq("SpeedDesired" := true)
  ) {
  override def applyTyped(
    instance: (VehicleData, AccidentData),
    distances: (Meters,Meters)
  ): ((VehicleData,AccidentData),(MpS,Meters,Meters)) = {

    val dt = 15 // seconds
    val ds = distances._1.v - distances._2.v // meters
    val vRelative = ds/dt // meters per second increase required over speedOther
    val v = instance._1.speedOther.toMpS.v + vRelative // actual speed required
    val dv = v - instance._1.speedNow.v // increase required over speedNow

    (instance, (MpS(v), distances._2, distances._1))
  }
}

object MakePack
  extends PairToPairPlanningModule[(VehicleData,AccidentData),Either[Meters,Meters], (VehicleData,AccidentData),Pack] {
  override def applyTyped(
    instance: (VehicleData, AccidentData),
    packable: Either[Meters,Meters]
  ): ((VehicleData,AccidentData),Pack) = {
    packable match {
      case Left(leftVal) => (instance, Pack(Some(leftVal), None))
      case Right(rightVal) => (instance, Pack(None, Some(rightVal)))
    }
  }
}

object PackLeft
  extends PairToPairPlanningModule[(VehicleData,AccidentData),Meters, (VehicleData,AccidentData),Pack](
    Seq("LeftPacked" !=:= true),
    Seq("LeftPacked" := true)
  ) {
  override def applyTyped(instance: (VehicleData, AccidentData), packable: Meters): ((VehicleData,AccidentData),Pack) = {
    (instance, Pack(Some(packable), None))
  }
}

object PackRight
  extends PairToPairPlanningModule[(VehicleData,AccidentData),Meters,(VehicleData,AccidentData),Pack](
    Seq("RightPacked" !=:= true),
    Seq("RightPacked" := true)
  ) {
  override def applyTyped(instance: (VehicleData, AccidentData), packable: Meters): ((VehicleData,AccidentData),Pack) = {
    (instance, Pack(None, Some(packable)))
  }
}

object MergePack extends PairToOnePlanningModule[Pack,Pack,Pack] {
  override def applyTyped(a: Pack, b: Pack): Pack = {
    Pack(a.left.orElse(b.left), a.right.orElse(b.right))
  }
}

object Unpack
  extends PairToPairPlanningModule[(VehicleData,AccidentData),Pack,(VehicleData,AccidentData),(Meters,Meters)](
    Seq("LeftPacked" =:= true, "RightPacked" =:= true),
    Seq("LeftPacked" := false, "RightPacked" := false)
  ) {
  override def applyTyped(
    instance: (VehicleData, AccidentData),
    pack: Pack
  ): ((VehicleData,AccidentData), (Meters,Meters)) = {
    (instance, (pack.left.get, pack.right.get))
  }
}

class MiddlePlanned(
  sc: SparkContext,
  server: DynamicServer,
  vehicles: RDD[VehicleData],
  accidents: RDD[AccidentData]
) extends PlannedPipeline {
  var distanceToOther:    PairToPairPlanningVariationPoint = _
  var notAlreadyPassed:    PairToOnePlanningVariationPoint = _
  var distanceToAccident: PairToPairPlanningVariationPoint = _
  var distanceDesired:    PairToPairPlanningVariationPoint = _
  var kmToM:              PairToPairPlanningVariationPoint = _

  var packLeft:     PairToPairPlanningVariationPoint = _
  var packRight:    PairToPairPlanningVariationPoint = _
  var mergePack:     PairToOnePlanningVariationPoint = _
  var unpack:       PairToPairPlanningVariationPoint = _
  var speedDesired: PairToPairPlanningVariationPoint = _

  var noOp1: PairToPairPlanningVariationPoint = _
  var noOp2: PairToPairPlanningVariationPoint = _
  var noOp3: PairToPairPlanningVariationPoint = _
  var noOp4: PairToPairPlanningVariationPoint = _
  var noOp5: PairToPairPlanningVariationPoint = _

  override def initializeVariationPoints(planner: DynamicPlanner): Unit = {
    distanceToOther    = new RESTPairToPairPlanningVariationPoint(server, planner, DistanceToOther)
    notAlreadyPassed   = new RESTPairToOnePlanningVariationPoint(server,  planner, NotAlreadyPassed)
    distanceToAccident = new RESTPairToPairPlanningVariationPoint(server, planner, DistanceToAccidentKM)
    distanceDesired    = new RESTPairToPairPlanningVariationPoint(server, planner, DistanceDesiredBasedOnAccident)
    kmToM              = new RESTPairToPairPlanningVariationPoint(server, planner, KilometersToMeters)

    packLeft     = new RESTPairToPairPlanningVariationPoint(server, planner, PackLeft)
    packRight    = new RESTPairToPairPlanningVariationPoint(server, planner, PackRight)
    mergePack    = new RESTPairToOnePlanningVariationPoint(server,  planner, MergePack)
    unpack       = new RESTPairToPairPlanningVariationPoint(server, planner, Unpack)
    speedDesired = new RESTPairToPairPlanningVariationPoint(server, planner, SpeedDesired)

    noOp1 = new RESTPairToPairPlanningVariationPoint(server, planner, null)
    noOp2 = new RESTPairToPairPlanningVariationPoint(server, planner, null)
    noOp3 = new RESTPairToPairPlanningVariationPoint(server, planner, null)
    noOp4 = new RESTPairToPairPlanningVariationPoint(server, planner, null)
    noOp5 = new RESTPairToPairPlanningVariationPoint(server, planner, null)
  }

  override def defaultModules: Seq[DynamicModule[_, _]] = ScenarioMiddle.DEFAULT_MODULES

  override def pipeline(planner: DynamicPlanner): Int = {
    innerPipeline(
      planner,
      distanceToOther,
      notAlreadyPassed,
      distanceToAccident,
      distanceDesired,
      kmToM,
      packLeft,
      packRight,
      mergePack,
      unpack,
      speedDesired,
      noOp1,
      noOp2,
      noOp3,
      noOp4,
      noOp5
    )
  }

  def innerPipeline(
    planner: DynamicPlanner,
    distanceToOther:    PairToPairPlanningVariationPoint,
    notAlreadyPassed:    PairToOnePlanningVariationPoint,
    distanceToAccident: PairToPairPlanningVariationPoint,
    distanceDesired:    PairToPairPlanningVariationPoint,
    kmToM:              PairToPairPlanningVariationPoint,
    packLeft:     PairToPairPlanningVariationPoint,
    packRight:    PairToPairPlanningVariationPoint,
    mergePack:     PairToOnePlanningVariationPoint,
    unpack:       PairToPairPlanningVariationPoint,
    speedDesired: PairToPairPlanningVariationPoint,
    noOp1: PairToPairPlanningVariationPoint,
    noOp2: PairToPairPlanningVariationPoint,
    noOp3: PairToPairPlanningVariationPoint,
    noOp4: PairToPairPlanningVariationPoint,
    noOp5: PairToPairPlanningVariationPoint
  ): Int = {
    implicit val dynamicPlanner = planner
    val defaults = Seq(
      "DistanceToOther" =:= false,
      "DistanceToAccident" =:= false,
      "DistanceDesired" =:= false,
      "SpeedDesired" =:= false,
      "LeftPacked" =:= false,
      "RightPacked" =:= false
    )

    val accidentJoin = vehicles
      .cartesian(accidents)
      .initialConditions()//(defaults: _*)
      .plannedFilter(f => notAlreadyPassed(f._1, f._2).asInstanceOf[Boolean], notAlreadyPassed)
      .withConditions(defaults: _*)

    val initialVehicleDB = accidentJoin

    val distNowToOther = initialVehicleDB.plannedMap(f => distanceToOther(f._1, f._2), distanceToOther)

    val distNowToAccident = accidentJoin.plannedMap(f => distanceToAccident(f._1, f._2), distanceToAccident)

    val distDesired = distNowToAccident.plannedMap(f => distanceDesired(f._1, f._2), distanceDesired)

    val c = distDesired.plannedMap(f => kmToM(f._1, f._2), kmToM)

    val convLeft = distNowToOther.plannedMap(f => packLeft(f._1, f._2), packLeft)
      .withConditions("LeftPacked" := true)
      .plannedMap(f => noOp1(f._1, f._2), noOp1)

    val convRight = c.plannedMap(f => packRight(f._1, f._2), packRight)
      .withConditions("RightPacked" := true)
      .plannedMap(f => noOp2(f._1, f._2), noOp2)

    val desiredSpeed = convLeft
      .plannedUnion(convRight, JoinConstraintResolution.Either)
      .plannedMap(f => noOp3(f._1, f._2), noOp3)
      .plannedReduceByKey((a: Any, b: Any) => mergePack(a, b), mergePack)
      .plannedMap(f => noOp4(f._1, f._2), noOp4)
      .plannedMap(f => unpack(f._1, f._2), unpack)
      .plannedMap(f => speedDesired(f._1, f._2), speedDesired)
      .plannedMap(f => noOp5(f._1, f._2), noOp5)

    desiredSpeed.goalConditions("SpeedDesired" =:= true)

    val result = desiredSpeed.plannedCollect[((VehicleData,AccidentData),(MpS,Meters,Meters))]()
    result.length
  }

  def update(planner: DynamicPlanner): Unit = {
    val jobId = planner.getLastJobId.get
    planner.registerModule(DistanceDesiredBasedOnSpeed)
    planner.removeModule(DistanceDesiredBasedOnAccident.getName)
    planner.replanPipeline(jobId)
  }
}
