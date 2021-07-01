package rugds.experiments.scenarios.simple

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import rugds.experiments.ExperimentBase

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ScenarioSimple {
  final val BUILDING_COPIES = 200

  final val DEFAULT_MODULES = Seq(
    ExtractOutsideTempF, ExtractInsideTempF, ExtractCurrentPowerW, ExpectedDemandFromF, PowerRequired, GetPower,
    ExtractOutsideTempC, ExtractInsideTempC
  )

  def makeBuildingDB(sc: SparkContext, count: Int): RDD[BuildingData] = {
    val buff = new ListBuffer[BuildingData]()
    for( itt <- 0 until count){
      buff ++= Seq(
        BuildingData(itt * 10 + 0, Map("rawInsideTemp" -> 1.0, "rawOutsideTemp" -> 1.0, "rawCurrentPower" -> 13.0)),
        BuildingData(itt * 10 + 1, Map("rawInsideTemp" -> 1.0, "rawOutsideTemp" -> 1.0, "rawCurrentPower" -> 13.0)),
        BuildingData(itt * 10 + 2, Map("rawInsideTemp" -> 1.0, "rawOutsideTemp" -> 1.0, "rawCurrentPower" -> 13.0)),
        BuildingData(itt * 10 + 3, Map("rawInsideTemp" -> 1.0, "rawOutsideTemp" -> 1.0, "rawCurrentPower" -> 13.0)),
        BuildingData(itt * 10 + 4, Map("rawInsideTemp" -> 1.0, "rawOutsideTemp" -> 1.0, "rawCurrentPower" -> 13.0)),
        BuildingData(itt * 10 + 5, Map("rawInsideTemp" -> 1.0, "rawOutsideTemp" -> 1.0, "rawCurrentPower" -> 13.0)),
        BuildingData(itt * 10 + 6, Map("rawInsideTemp" -> 1.0, "rawOutsideTemp" -> 1.0, "rawCurrentPower" -> 13.0)),
        BuildingData(itt * 10 + 7, Map("rawInsideTemp" -> 1.0, "rawOutsideTemp" -> 1.0, "rawCurrentPower" -> 13.0)),
        BuildingData(itt * 10 + 8, Map("rawInsideTemp" -> 1.0, "rawOutsideTemp" -> 1.0, "rawCurrentPower" -> 13.0)),
        BuildingData(itt * 10 + 9, Map("rawInsideTemp" -> 1.0, "rawOutsideTemp" -> 1.0, "rawCurrentPower" -> 13.0)),
        BuildingData(itt * 10 + 10, Map("rawInsideTemp" -> 1.0, "rawOutsideTemp" -> 1.0, "rawCurrentPower" -> 13.0))
      )
    }
    sc.parallelize(buff, Math.max(sc.defaultParallelism, Math.round(count / 100)))
  }
}
