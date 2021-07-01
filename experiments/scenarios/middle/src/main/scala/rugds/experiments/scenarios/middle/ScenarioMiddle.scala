package rugds.experiments.scenarios.middle

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer

object ScenarioMiddle {
  final val VEHICLE_COPIES = 100
  final val ACCIDENT_COPIES = 1

  final val DEFAULT_MODULES = Seq(
    DistanceToOther, NotAlreadyPassed, DistanceToAccidentKM, DistanceDesiredBasedOnAccident,
    KilometersToMeters, SpeedDesired, MakePack, MergePack, Unpack, PackLeft, PackRight
  )

  /**
    * The Middle scenario starts with a cartesian, so for both collections the number of records processed during a
    * benchmark of `iterations` with `multiplier` equal to the current iteration:
    * {{{(VEHICLES * multiplier * ACCIDENTS * multiplier) * (iterations/multiplier)}}}
    *
    * where without the cartesian the number of records processed would be:
    * {{{(VEHICLES * multipleir + ACCIDENTS * multiplier) * (iterations/multiplier)}}}
    *
    * As such we must take the root of the multiplier to maintain a constant number of records over all iterations.
    *
    * @param base the base record count for this collection
    * @param multiplier the multiplier to apply to it
    * @return the scaled record count for the collection
    */
  def applyMultiplier(base: Int, multiplier: Int): Int = {
    (base * Math.sqrt(multiplier)).toInt
  }

  def makeVehicleDB(sc: SparkContext, count: Int): RDD[VehicleData] = {
    val buff = new ListBuffer[VehicleData]()
    for( itt <- 0 until count ){
      buff ++= Seq(
        VehicleData(itt * 10 + 0, KMpH(55), KMpH(45), Meters(10), Meters(500)),
        VehicleData(itt * 10 + 1, KMpH(55), KMpH(55), Meters(10), Meters(500)),
        VehicleData(itt * 10 + 2, KMpH(55), KMpH(65), Meters(10), Meters(500)),
        VehicleData(itt * 10 + 3, KMpH(55), KMpH(55), Meters(490), Meters(500)),
        VehicleData(itt * 10 + 4, KMpH(55), KMpH(55), Meters(480), Meters(500)),
        VehicleData(itt * 10 + 5, KMpH(55), KMpH(55), Meters(470), Meters(500)),
        VehicleData(itt * 10 + 6, KMpH(55), KMpH(55), Meters(100), Meters(2000)),
        VehicleData(itt * 10 + 7, KMpH(55), KMpH(55), Meters(100), Meters(2000)),
        VehicleData(itt * 10 + 8, KMpH(55), KMpH(55), Meters(2000), Meters(100)),
        VehicleData(itt * 10 + 9, KMpH(55), KMpH(55), Meters(690), Meters(700)),
        VehicleData(itt * 10 + 10, KMpH(55), KMpH(55), Meters(990), Meters(1000))
      )
    }
    sc.parallelize(buff, Math.max(sc.defaultParallelism, Math.round(count / 10)))
  }

  def makeAccidentDB(sc: SparkContext, count: Int): RDD[AccidentData] = {
    val buff = new ListBuffer[AccidentData]()
    for( itt <- 0 until count ){
      buff ++= Seq(
        AccidentData(itt * 2 + 0, Meters(1000)),
        AccidentData(itt * 2 + 1, Meters(8000))
      )
    }
    sc.parallelize(buff, Math.max(sc.defaultParallelism, Math.round(count / 2)))
  }
}
