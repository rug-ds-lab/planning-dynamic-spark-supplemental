package rugds.experiments.scenarios.simple

import org.apache.spark.SparkContext
import org.apache.spark.dynamic.DynamicVariationPoint
import org.apache.spark.dynamic.rest.{DynamicServer, RESTVariationPoint}
import org.apache.spark.dynamic.rdd.DynamicRDDFunctions._
import org.apache.spark.rdd.RDD
import rugds.experiments.DynamicPipeline

object DynamicFuncs {
  val extractOutsideTempF = (building: BuildingData) => {
    building.data.get("rawOutsideTemp") match {
      case Some(raw: Double) => BuildingData(building.buildingId, building.data + ("outsideTemp" -> Fahrenheit(raw * 3)))
      case _ =>
        throw new Exception("rawOutsideTemp")
        building
    }
  }

  val extractOutsideTempC = (building: BuildingData) => {
    building.data.get("rawOutsideTemp") match {
      case Some(raw: Double) => BuildingData(building.buildingId, building.data + ("outsideTemp" -> Celsius(((raw * 3) - 32) * 5 / 9)))
      case _ =>
        throw new Exception("rawOutsideTemp")
        building
    }
  }

  val extractInsideTempF = (building: BuildingData) => {
    building.data.get("rawInsideTemp") match {
      case Some(raw: Double) => BuildingData(building.buildingId, building.data + ("insideTemp" -> Fahrenheit(raw * 3)))
      case _ =>
        throw new Exception("rawInsideTemp")
        building
    }
  }

  val extractInsideTempC = (building: BuildingData) => {
    building.data.get("rawInsideTemp") match {
      case Some(raw: Double) => BuildingData(building.buildingId, building.data + ("insideTemp" -> Celsius(((raw * 3) - 32) * 5 / 9)))
      case _ =>
        throw new Exception("rawInsideTemp")
        building
    }
  }

  val extractCurrentPowerW = (building: BuildingData) => {
    building.data.get("rawCurrentPower") match {
      case Some(raw: Double) => BuildingData(building.buildingId, building.data + ("currentPower" -> Watts(raw * 3 + 5)))
      case _ =>
        throw new Exception("rawCurrentPower")
        building
    }
  }

  val expectedDemandFromF = (building: BuildingData) => {
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

  val expectedDemandFromC = (building: BuildingData) => {
    val outside = building.data.get("outsideTemp")
    val inside = building.data.get("insideTemp")

    (outside, inside) match {
      case (Some(o: Celsius), Some(i: Celsius)) =>
        val ideal = Celsius(22)
        val difference = Celsius(ideal.v - o.v) // Scenario: warmer outside temperature means warmer ideal temperature
        val target = Celsius(ideal.v - (difference.v * 0.5 / 9))
        val result = Celsius(Math.max(target.v - i.v, 0)) // No negative demand
        BuildingData(building.buildingId, building.data + ("expectedDemand" -> result))
      case _ =>
        throw new Exception("outsideTemp or insideTemp")
        building
    }
  }

  val powerRequired = (building: BuildingData) => {
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

  val getPower = (building: BuildingData) => {
    (building.buildingId, building.data.get("powerRequired").asInstanceOf[Option[Watts]])
  }
}

class SimpleDynamic(sc: SparkContext, server: DynamicServer, buildings: RDD[BuildingData]) extends DynamicPipeline {
  var getOutsideTemp:          DynamicVariationPoint[(BuildingData) => BuildingData]          = _
  var getInsideTemp:           DynamicVariationPoint[(BuildingData) => BuildingData]          = _
  var getCurrentPower:         DynamicVariationPoint[(BuildingData) => BuildingData]          = _
  var calculateExpectedDemand: DynamicVariationPoint[(BuildingData) => BuildingData]          = _
  var calculatePowerRequired:  DynamicVariationPoint[(BuildingData) => BuildingData]          = _
  var extractPower:            DynamicVariationPoint[(BuildingData) => (Long, Option[Watts])] = _

  override def initializeVariationPoints(): Unit = {
    getOutsideTemp          = new RESTVariationPoint(server, "getOutsideTemp",          0, DynamicFuncs.extractOutsideTempF)
    getInsideTemp           = new RESTVariationPoint(server, "getInsideTemp",           0, DynamicFuncs.extractInsideTempF)
    getCurrentPower         = new RESTVariationPoint(server, "getCurrentPower",         0, DynamicFuncs.extractCurrentPowerW)
    calculateExpectedDemand = new RESTVariationPoint(server, "calculateExpectedDemand", 0, DynamicFuncs.expectedDemandFromF)
    calculatePowerRequired  = new RESTVariationPoint(server, "calculatePowerRequired",  0, DynamicFuncs.powerRequired)
    extractPower            = new RESTVariationPoint(server, "extractPower",            0, DynamicFuncs.getPower)
  }

  override def pipeline(): Int = {
    innerPipeline(
      getOutsideTemp,
      getInsideTemp,
      getCurrentPower,
      calculateExpectedDemand,
      calculatePowerRequired,
      extractPower
    )
  }

  def innerPipeline(
    getOutsideTemp:          DynamicVariationPoint[(BuildingData) => BuildingData],
    getInsideTemp:           DynamicVariationPoint[(BuildingData) => BuildingData],
    getCurrentPower:         DynamicVariationPoint[(BuildingData) => BuildingData],
    calculateExpectedDemand: DynamicVariationPoint[(BuildingData) => BuildingData],
    calculatePowerRequired:  DynamicVariationPoint[(BuildingData) => BuildingData],
    extractPower:            DynamicVariationPoint[(BuildingData) => (Long, Option[Watts])]
  ): Int = {
    val withOutsideTemp = buildings.dynamicMap(f => getOutsideTemp.value(f), getOutsideTemp)
    val withInsideTemp = withOutsideTemp.dynamicMap(f => getInsideTemp.value(f), getInsideTemp)
    val withDemand = withInsideTemp.dynamicMap(f => calculateExpectedDemand.value(f), calculateExpectedDemand)
    val withCurrentPower = withDemand.dynamicMap(f => getCurrentPower.value(f), getCurrentPower)
    val withPowerRequired = withCurrentPower.dynamicMap(f => calculatePowerRequired.value(f), calculatePowerRequired)
    val extracted = withPowerRequired.dynamicMap(f => extractPower.value(f), extractPower)

    val result = extracted.collect()

    result.length
  }

  def update(): Unit = {
    getOutsideTemp.write(DynamicFuncs.extractOutsideTempC)
    getInsideTemp.write(DynamicFuncs.extractInsideTempC)
    calculateExpectedDemand.write(DynamicFuncs.expectedDemandFromC)
  }
}
