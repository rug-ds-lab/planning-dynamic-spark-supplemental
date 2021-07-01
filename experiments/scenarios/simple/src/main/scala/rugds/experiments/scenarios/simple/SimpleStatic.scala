package rugds.experiments.scenarios.simple

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import rugds.experiments.StaticPipeline

//object DynamicFuncs {
//  val extractOutsideTempC = (building: BuildingData) => {
//    building.data.get("rawOutsideTemp") match {
//      case Some(raw: Double) => BuildingData(building.buildingId, building.data + ("outsideTemp" -> Celsius(((raw * 3) - 32) * 5 / 9)))
//      case _ =>
//        throw new Exception("rawOutsideTemp")
//        building
//    }
//  }
//
//  val extractInsideTempC = (building: BuildingData) => {
//    building.data.get("rawInsideTemp") match {
//      case Some(raw: Double) => BuildingData(building.buildingId, building.data + ("insideTemp" -> Celsius(((raw * 3) - 32) * 5 / 9)))
//      case _ =>
//        throw new Exception("rawInsideTemp")
//        building
//    }
//  }
//
//  val expectedDemandFromC = (building: BuildingData) => {
//    val outside = building.data.get("outsideTemp")
//    val inside = building.data.get("insideTemp")
//
//    (outside, inside) match {
//      case (Some(o: Celsius), Some(i: Celsius)) =>
//        val ideal = Celsius(22)
//        val difference = Celsius(ideal.v - o.v) // Scenario: warmer outside temperature means warmer ideal temperature
//      val target = Celsius(ideal.v - (difference.v * 0.5 / 9))
//        val result = Celsius(Math.max(target.v - i.v, 0)) // No negative demand
//        BuildingData(building.buildingId, building.data + ("expectedDemand" -> result))
//      case _ =>
//        throw new Exception("outsideTemp or insideTemp")
//        building
//    }
//  }
//}

class SimpleStatic(sc: SparkContext, buildings: RDD[BuildingData]) extends StaticPipeline {
  override def pipeline(): Int = {
    val withOutsideTemp = buildings.map { building =>
      building.data.get("rawOutsideTemp") match {
        case Some(raw: Double) => BuildingData(building.buildingId, building.data + ("outsideTemp" -> Fahrenheit(raw * 3)))
        case _ =>
          throw new Exception("rawOutsideTemp")
          building
      }
    }
    val withInsideTemp = withOutsideTemp.map { building =>
      building.data.get("rawInsideTemp") match {
        case Some(raw: Double) => BuildingData(building.buildingId, building.data + ("insideTemp" -> Fahrenheit(raw * 3)))
        case _ =>
          throw new Exception("rawInsideTemp")
          building
      }
    }
    val withDemand = withInsideTemp.map { building =>
      building.data.get("rawCurrentPower") match {
        case Some(raw: Double) => BuildingData(building.buildingId, building.data + ("currentPower" -> Watts(raw * 3 + 5)))
        case _ =>
          throw new Exception("rawCurrentPower")
          building
      }
    }
    val withCurrentPower = withDemand.map { building =>
      val outside = building.data.get("outsideTemp")
      val inside = building.data.get("insideTemp")

      (outside, inside) match {
        case (Some(o: Fahrenheit), Some(i: Fahrenheit)) =>
          val ideal = Fahrenheit(70)
          val difference = Fahrenheit(ideal.v - o.v) // Scenario: warmer outside temperature means warmer ideal temperature
        val target = Fahrenheit(ideal.v - (difference.v * 0.1))
          val result = Fahrenheit(Math.max(target.v - i.v, 0)) // No negative demand
          BuildingData(building.buildingId, building.data + ("expectedDemand" -> result))
        case _ =>
          throw new Exception("outsideTemp or insideTemp")
          building
      }
    }
    val withPowerRequired = withCurrentPower.map { building =>
      val currentPower = building.data.get("currentPower")
      val expectedDemand = building.data.get("expectedDemand")

      (currentPower, expectedDemand) match {
        case (Some(current: Watts), Some(demand: Fahrenheit)) =>
          val result = Watts(current.v + demand.v * 3 + 5)
          BuildingData(building.buildingId, building.data + ("powerRequired" -> result))
        case (Some(current: Watts), Some(demand: Celsius)) =>
          val result = Watts(current.v + (demand.v- 32) * (5/9) * 3 + 5)
          BuildingData(building.buildingId, building.data + ("powerRequired" -> result))
        case _ =>
          throw new Exception("currentPower or expectedDemand")
          building
      }
    }
    val extracted = withPowerRequired.map { building =>
      (building.buildingId, building.data.get("powerRequired").asInstanceOf[Option[Watts]])
    }

    val result = extracted.collect()

    result.length
  }

  def update(): Unit = ???
}
