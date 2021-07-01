package rugds.experiments.scenarios.middle

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import rugds.experiments.StaticPipeline

//object DynamicFuncs {
//  val distanceToAccidentM = (vehicle: VehicleData, accident: AccidentData) => {
//    ((vehicle, accident), Meters(accident.locationAccident.v - vehicle.locationNow.v))
//  }
//
//  val distanceDesiredBasedOnSpeed = (vehicle: VehicleData, accident: AccidentData) => {
//    ((vehicle, accident), Meters(Math.max(100, (vehicle.speedNow.v + vehicle.speedNow.v/ 2) * 0.9)))
//  }
//}

class MiddleStatic(
  sc: SparkContext,
  vehicles: RDD[VehicleData],
  accidents: RDD[AccidentData]
) extends StaticPipeline {
  override def pipeline(): Int = {
    val accidentJoin = vehicles
      .cartesian(accidents)
      .filter(f => f._2.locationAccident.v >= f._1.locationNow.v && f._1.locationNow.v < f._1.locationOther.v)

    val initialVehicleDB = accidentJoin

    val distNowToOther = initialVehicleDB.map(f => ((f._1, f._2), Meters(f._1.locationOther.v - f._1.locationNow.v)))

    val distNowToAccident = accidentJoin.map(f => ((f._1, f._2), Kilometers((f._2.locationAccident.v - f._1.locationNow.v) / 1000)))

    val distDesired = distNowToAccident.map { f =>
      (f._1,
        if (f._2.v > Kilometers(2).v)
          Kilometers(0.02)
        else if (f._2.v > Kilometers(0.5).v)
          Kilometers(0.03)
        else
          Kilometers(0.035)
      )
    }
    val c = distDesired.map(f => (f._1, Meters(f._2.v * 1000)))

    val convLeft = distNowToOther.map(f => (f._1, Pack(Some(f._2), None)))
      .map(f => (f._1, f._2))

    val convRight = c.map(f => (f._1, Pack(None, Some(f._2))))
      .map(f => (f._1, f._2))

    val desiredSpeed = convLeft
      .union(convRight)
      .map(f => (f._1, f._2))
      .reduceByKey((a: Pack, b: Pack) => Pack(a.left.orElse(b.left), a.right.orElse(b.right)))
      .map(f => (f._1, f._2))
      .map(f => (f._1, (f._2.left.get, f._2.right.get)))
      .map { f =>
        val dt = 15 // seconds
        val ds = f._2._1.v - f._2._2.v // meters
        val vRelative = ds / dt // meters per second increase required over speedOther
        val v = f._1._1.speedOther.toMpS.v + vRelative // actual speed required
        val dv = v - f._1._1.speedNow.v // increase required over speedNow

        (f._1, (MpS(v), f._2._2, f._2._1))
      }
      .map(f => (f._1, f._2))

    val result = desiredSpeed.collect()
    result.length
  }

  def update(): Unit = ???
}
