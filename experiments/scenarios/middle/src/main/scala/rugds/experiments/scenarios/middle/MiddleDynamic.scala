package rugds.experiments.scenarios.middle

import org.apache.spark.SparkContext
import org.apache.spark.dynamic.DynamicVariationPoint
import org.apache.spark.dynamic.rest.{DynamicServer, RESTVariationPoint}
import org.apache.spark.dynamic.rdd.DynamicRDDFunctions._
import org.apache.spark.rdd.RDD
import rugds.experiments.DynamicPipeline

object DynamicFuncs {
  val distanceToOther = (vehicle: VehicleData, accident: AccidentData) => {
    ((vehicle, accident), Meters(vehicle.locationOther.v - vehicle.locationNow.v))
  }

  val notAlreadyPassed = (vehicle: VehicleData, accident: AccidentData) => {
    accident.locationAccident.v >= vehicle.locationNow.v && vehicle.locationNow.v < vehicle.locationOther.v
  }

  val distanceToAccidentKM = (vehicle: VehicleData, accident: AccidentData) => {
    ((vehicle, accident), Kilometers((accident.locationAccident.v - vehicle.locationNow.v) / 1000))
  }

  val distanceDesiredBasedOnAccident = (instance: (VehicleData, AccidentData), distance: Kilometers) => {
    (instance,
      if      (distance.v > Kilometers(2).v)
        Kilometers(0.02)
      else if (distance.v > Kilometers(0.5).v)
        Kilometers(0.03)
      else
        Kilometers(0.035)
    )
  }

  val distanceDesiredBasedOnSpeed = (vehicle: VehicleData, accident: AccidentData) => {
    ((vehicle, accident), Meters(Math.max(100, (vehicle.speedNow.v + vehicle.speedNow.v/ 2) * 0.9)))
  }

  val kilometersToMeters = (instance: (VehicleData, AccidentData), distance: Kilometers) => {
    (instance, Meters(distance.v * 1000))
  }

  val speedDesired = (instance: (VehicleData, AccidentData), distances: (Meters,Meters)) => {

    val dt = 15 // seconds
    val ds = distances._1.v - distances._2.v // meters
    val vRelative = ds/dt // meters per second increase required over speedOther
    val v = instance._1.speedOther.toMpS.v + vRelative // actual speed required
    val dv = v - instance._1.speedNow.v // increase required over speedNow

    (instance, (MpS(v), distances._2, distances._1))
  }

  val packLeft = (instance: (VehicleData, AccidentData), packable: Meters) => {
    (instance, Pack(Some(packable), None))
  }

  val packRight = (instance: (VehicleData, AccidentData), packable: Meters) => {
    (instance, Pack(None, Some(packable)))
  }

  val mergePack = (a: Pack, b: Pack) => {
    Pack(a.left.orElse(b.left), a.right.orElse(b.right))
  }

  val unPack = (instance: (VehicleData, AccidentData), pack: Pack) => {
    (instance, (pack.left.get, pack.right.get))
  }

  val noOpPack = (instance: (VehicleData,AccidentData), pack: Pack) => {
    (instance, pack)
  }

  val noOpMpsMetersMeters = (instance: (VehicleData,AccidentData), mps: (MpS,Meters,Meters)) => {
    (instance, mps)
  }
}

class MiddleDynamic(
  sc: SparkContext,
  server: DynamicServer,
  vehicles: RDD[VehicleData],
  accidents: RDD[AccidentData]
) extends DynamicPipeline {
  var distanceToOther:    DynamicVariationPoint[(VehicleData, AccidentData) => ((VehicleData, AccidentData), Meters)]                   = _
  var notAlreadyPassed:   DynamicVariationPoint[(VehicleData, AccidentData) => Boolean]                                                 = _
  var distanceToAccident: DynamicVariationPoint[(VehicleData, AccidentData) => ((VehicleData, AccidentData), Kilometers)]               = _
  var distanceDesired:    DynamicVariationPoint[((VehicleData, AccidentData), Kilometers) => ((VehicleData, AccidentData), Kilometers)] = _
  var kmToM:              DynamicVariationPoint[((VehicleData, AccidentData), Kilometers) => ((VehicleData, AccidentData), Meters)]     = _

  var packLeft:     DynamicVariationPoint[((VehicleData, AccidentData), Meters) => ((VehicleData, AccidentData), Pack)]                            = _
  var packRight:    DynamicVariationPoint[((VehicleData, AccidentData), Meters) => ((VehicleData, AccidentData), Pack)]                            = _
  var mergePack:    DynamicVariationPoint[(Pack, Pack) => Pack]                                                                                    = _
  var unpack:       DynamicVariationPoint[((VehicleData, AccidentData), Pack) => ((VehicleData, AccidentData), (Meters, Meters))]                  = _
  var speedDesired: DynamicVariationPoint[((VehicleData, AccidentData), (Meters, Meters)) => ((VehicleData, AccidentData), (MpS, Meters, Meters))] = _

  var noOp1: DynamicVariationPoint[((VehicleData, AccidentData), Pack) => ((VehicleData, AccidentData), Pack)]                                   = _
  var noOp2: DynamicVariationPoint[((VehicleData, AccidentData), Pack) => ((VehicleData, AccidentData), Pack)]                                   = _
  var noOp3: DynamicVariationPoint[((VehicleData, AccidentData), Pack) => ((VehicleData, AccidentData), Pack)]                                   = _
  var noOp4: DynamicVariationPoint[((VehicleData, AccidentData), Pack) => ((VehicleData, AccidentData), Pack)]                                   = _
  var noOp5: DynamicVariationPoint[((VehicleData, AccidentData), (MpS, Meters, Meters)) => ((VehicleData, AccidentData), (MpS, Meters, Meters))] = _

  override def initializeVariationPoints(): Unit = {
    distanceToOther    = new RESTVariationPoint(server, "distanceToOther",    0, DynamicFuncs.distanceToOther)
    notAlreadyPassed   = new RESTVariationPoint(server, "notAlreadyPassed",   0, DynamicFuncs.notAlreadyPassed)
    distanceToAccident = new RESTVariationPoint(server, "distanceToAccident", 0, DynamicFuncs.distanceToAccidentKM)
    distanceDesired    = new RESTVariationPoint(server, "distanceDesired",    0, DynamicFuncs.distanceDesiredBasedOnAccident)
    kmToM              = new RESTVariationPoint(server, "kmToM",              0, DynamicFuncs.kilometersToMeters)

    packLeft     = new RESTVariationPoint(server, "packLeft",     0, DynamicFuncs.packLeft)
    packRight    = new RESTVariationPoint(server, "packRight",    0, DynamicFuncs.packRight)
    mergePack    = new RESTVariationPoint(server, "mergePack",    0, DynamicFuncs.mergePack)
    unpack       = new RESTVariationPoint(server, "unpack",       0, DynamicFuncs.unPack)
    speedDesired = new RESTVariationPoint(server, "speedDesired", 0, DynamicFuncs.speedDesired)

    noOp1 = new RESTVariationPoint(server, "noOp1", 0, DynamicFuncs.noOpPack)
    noOp2 = new RESTVariationPoint(server, "noOp2", 0, DynamicFuncs.noOpPack)
    noOp3 = new RESTVariationPoint(server, "noOp3", 0, DynamicFuncs.noOpPack)
    noOp4 = new RESTVariationPoint(server, "noOp4", 0, DynamicFuncs.noOpPack)
    noOp5 = new RESTVariationPoint(server, "noOp5", 0, DynamicFuncs.noOpMpsMetersMeters)
  }

  override def pipeline(): Int = {
    innerPipeline(
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

  // TODO distanceToAccident: ((VehicleData, AccidentData), Meters)
  // TODO distanceDesired: ((VehicleData,AccidentData), Meters) => ((VehicleData,AccidentData),Meters)
  def innerPipeline(
    distanceToOther:    DynamicVariationPoint[(VehicleData, AccidentData) => ((VehicleData, AccidentData), Meters)],
    notAlreadyPassed:   DynamicVariationPoint[(VehicleData, AccidentData) => Boolean],
    distanceToAccident: DynamicVariationPoint[(VehicleData, AccidentData) => ((VehicleData, AccidentData), Kilometers)],
    distanceDesired:    DynamicVariationPoint[((VehicleData, AccidentData), Kilometers) => ((VehicleData, AccidentData), Kilometers)],
    kmToM:              DynamicVariationPoint[((VehicleData, AccidentData), Kilometers) => ((VehicleData, AccidentData), Meters)],
    packLeft:     DynamicVariationPoint[((VehicleData, AccidentData), Meters) => ((VehicleData, AccidentData), Pack)],
    packRight:    DynamicVariationPoint[((VehicleData, AccidentData), Meters) => ((VehicleData, AccidentData), Pack)],
    mergePack:    DynamicVariationPoint[(Pack, Pack) => Pack],
    unpack:       DynamicVariationPoint[((VehicleData, AccidentData), Pack) => ((VehicleData, AccidentData), (Meters, Meters))],
    speedDesired: DynamicVariationPoint[((VehicleData, AccidentData), (Meters, Meters)) => ((VehicleData, AccidentData), (MpS, Meters, Meters))],
    noOp1: DynamicVariationPoint[((VehicleData, AccidentData), Pack) => ((VehicleData, AccidentData), Pack)],
    noOp2: DynamicVariationPoint[((VehicleData, AccidentData), Pack) => ((VehicleData, AccidentData), Pack)],
    noOp3: DynamicVariationPoint[((VehicleData, AccidentData), Pack) => ((VehicleData, AccidentData), Pack)],
    noOp4: DynamicVariationPoint[((VehicleData, AccidentData), Pack) => ((VehicleData, AccidentData), Pack)],
    noOp5: DynamicVariationPoint[((VehicleData, AccidentData), (MpS, Meters, Meters)) => ((VehicleData, AccidentData), (MpS, Meters, Meters))]
  ): Int = {
    val accidentJoin = vehicles
      .cartesian(accidents)
      .dynamicFilter(f => notAlreadyPassed.value(f._1, f._2), notAlreadyPassed)

    val initialVehicleDB = accidentJoin

    val distNowToOther = initialVehicleDB.dynamicMap(f => distanceToOther.value(f._1, f._2), distanceToOther)

    val distNowToAccident = accidentJoin.dynamicMap(f => distanceToAccident.value(f._1, f._2), distanceToAccident)

    val distDesired = distNowToAccident.dynamicMap(f => distanceDesired.value(f._1, f._2), distanceDesired)

    val c = distDesired.dynamicMap(f => kmToM.value(f._1, f._2), kmToM)

    val convLeft = distNowToOther.dynamicMap(f => packLeft.value(f._1, f._2), packLeft)
      .dynamicMap(f => noOp1.value(f._1, f._2), noOp1)

    val convRight = c.dynamicMap(f => packRight.value(f._1, f._2), packRight)
      .dynamicMap(f => noOp2.value(f._1, f._2), noOp2)

    val desiredSpeed = convLeft
      .union(convRight)
      .dynamicMap(f => noOp3.value(f._1, f._2), noOp3)
      .dynamicReduceByKey((a: Pack, b: Pack) => mergePack.value(a, b), mergePack)
      .dynamicMap(f => noOp4.value(f._1, f._2), noOp4)
      .dynamicMap(f => unpack.value(f._1, f._2), unpack)
      .dynamicMap(f => speedDesired.value(f._1, f._2), speedDesired)
      .dynamicMap(f => noOp5.value(f._1, f._2), noOp5)

    val result = desiredSpeed.collect()
    result.length
  }

  def update(): Unit = ???
  // Not possible because the types are different between RiskAssessment and Seq[RiskAssessment]
//  {
//    distanceDesired.write(DynamicFuncs.distanceDesiredBasedOnSpeed)
//  }
}
