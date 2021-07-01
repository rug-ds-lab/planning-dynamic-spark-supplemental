package rugds.experiments.scenarios.simple

import org.apache.spark.SparkContext
import org.apache.spark.dynamic.rest.DynamicServer
import org.apache.spark.dynamic.planning._
import org.apache.spark.dynamic.planning.rest._
import org.apache.spark.dynamic.planning.PlanningRDDFunctions._
import org.apache.spark.dynamic.planning.modules._
import org.apache.spark.dynamic.planning.variationpoints._
import org.apache.spark.dynamic.planning.variables.VariableAssignment._
import org.apache.spark.rdd.RDD
import rugds.experiments.PlannedPipeline

object ExtractOutsideTempF
  extends OneToOnePlanningModule[BuildingData,BuildingData](effects = Seq("ExtractedOutsideTempF" := true)) {
  override def applyTyped(building: BuildingData): BuildingData = {
    building.data.get("rawOutsideTemp") match {
      case Some(raw: Double) => BuildingData(building.buildingId, building.data + ("outsideTemp" -> Fahrenheit(raw * 3)))
      case _ =>
        throw new Exception("rawOutsideTemp")
        building
    }
  }
}

object ExtractOutsideTempC
  extends OneToOnePlanningModule[BuildingData,BuildingData](effects = Seq("ExtractedOutsideTempC" := true)) {
  override def applyTyped(building: BuildingData): BuildingData = {
    building.data.get("rawOutsideTemp") match {
      case Some(raw: Double) => BuildingData(building.buildingId, building.data + ("outsideTemp" -> Celsius(((raw * 3) - 32) * 5 / 9)))
      case _ =>
        throw new Exception("rawOutsideTemp")
        building
    }
  }
}


object ExtractInsideTempF
  extends OneToOnePlanningModule[BuildingData,BuildingData](effects = Seq("ExtractedInsideTempF" := true)) {
  override def applyTyped(building: BuildingData): BuildingData = {
    building.data.get("rawInsideTemp") match {
      case Some(raw: Double) => BuildingData(building.buildingId, building.data + ("insideTemp" -> Fahrenheit(raw * 3)))
      case _ =>
        throw new Exception("rawInsideTemp")
        building
    }
  }
}


object ExtractInsideTempC
  extends OneToOnePlanningModule[BuildingData,BuildingData](effects = Seq("ExtractedInsideTempC" := true)) {
  override def applyTyped(building: BuildingData): BuildingData = {
    building.data.get("rawInsideTemp") match {
      case Some(raw: Double) => BuildingData(building.buildingId, building.data + ("insideTemp" -> Celsius(((raw * 3) - 32) * 5 / 9)))
      case _ =>
        throw new Exception("rawInsideTemp")
        building
    }
  }
}

object ExtractCurrentPowerW
  extends OneToOnePlanningModule[BuildingData,BuildingData](effects = Seq("ExtractedCurrentPowerW" := true)) {
  override def applyTyped(building: BuildingData): BuildingData = {
    building.data.get("rawCurrentPower") match {
      case Some(raw: Double) => BuildingData(building.buildingId, building.data + ("currentPower" -> Watts(raw * 3 + 5)))
      case _ =>
        throw new Exception("rawCurrentPower")
        building
    }
  }
}

object ExpectedDemandFromF
  extends OneToOnePlanningModule[BuildingData,BuildingData](
    Seq("ExtractedOutsideTempF" =:= true, "ExtractedInsideTempF" =:= true),
    Seq("ExpectedDemandF" := true)
  ) {
  override def applyTyped(building: BuildingData): BuildingData = {
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
}

object ExpectedDemandFromC
  extends OneToOnePlanningModule[BuildingData,BuildingData](
    Seq("ExtractedOutsideTempC" =:= true, "ExtractedInsideTempC" =:= true),
    Seq("ExpectedDemandC" := true)
  ) {
  override def applyTyped(building: BuildingData): BuildingData = {
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
}

object PowerRequired
  extends OneToOnePlanningModule[BuildingData,BuildingData](
    Seq("ExpectedDemandF" =:= true |:| "ExpectedDemandC" =:= true, "ExtractedCurrentPowerW" =:= true),
    Seq("PowerRequired" := true)
  ) {
  override def applyTyped(building: BuildingData): BuildingData = {
    val currentPower = building.data.get("currentPower")
    val expectedDemand = building.data.get("expectedDemand")

    (currentPower, expectedDemand) match {
      case (Some(current: Watts), Some(demand: Fahrenheit)) =>
        val result = Watts(current.v + demand.v * 3 + 5)
        BuildingData(building.buildingId, building.data + ("powerRequired" -> result))
      case (Some(current: Watts), Some(demand: Celsius)) =>
        val result = Watts(current.v + (demand.v - 32) * (5/9) * 3 + 5)
        BuildingData(building.buildingId, building.data + ("powerRequired" -> result))
      case _ =>
        throw new Exception("currentPower or expectedDemand")
        building
    }
  }
}

object GetPower
  extends OneToPairPlanningModule[BuildingData, Long, Option[Watts]](
    preconditions = Seq("PowerRequired" =:= true)
  ) {
  override def applyTyped(building: BuildingData): (Long, Option[Watts]) = {
    (building.buildingId, building.data.get("powerRequired").asInstanceOf[Option[Watts]])
  }
}

class SimplePlanned(sc: SparkContext, server: DynamicServer, buildings: RDD[BuildingData]) extends PlannedPipeline {
  var getOutsideTemp:           OneToOnePlanningVariationPoint = _
  var getInsideTemp:            OneToOnePlanningVariationPoint = _
  var calculateExpectedDemand:  OneToOnePlanningVariationPoint = _
  var getCurrentPower:          OneToOnePlanningVariationPoint = _
  var calculatePowerRequired:   OneToOnePlanningVariationPoint = _
  var extractPower:            OneToPairPlanningVariationPoint = _

  override def defaultModules: Seq[DynamicModule[_, _]] = ScenarioSimple.DEFAULT_MODULES

  override def initializeVariationPoints(planner: DynamicPlanner): Unit = {
    getOutsideTemp          = new RESTOneToOnePlanningVariationPoint(server,  planner, ExtractOutsideTempF)
    getInsideTemp           = new RESTOneToOnePlanningVariationPoint(server,  planner, ExtractInsideTempF)
    calculateExpectedDemand = new RESTOneToOnePlanningVariationPoint(server,  planner, ExpectedDemandFromF)
    getCurrentPower         = new RESTOneToOnePlanningVariationPoint(server,  planner, ExtractCurrentPowerW)
    calculatePowerRequired  = new RESTOneToOnePlanningVariationPoint(server,  planner, PowerRequired)
    extractPower            = new RESTOneToPairPlanningVariationPoint(server, planner, GetPower)
  }

  override def pipeline(planner: DynamicPlanner): Int = {
    innerPipeline(
      planner,
      getOutsideTemp,
      getInsideTemp,
      calculateExpectedDemand,
      getCurrentPower,
      calculatePowerRequired,
      extractPower
    )
  }

  def innerPipeline(
    planner: DynamicPlanner,
    getOutsideTemp:           OneToOnePlanningVariationPoint,
    getInsideTemp:            OneToOnePlanningVariationPoint,
    calculateExpectedDemand:  OneToOnePlanningVariationPoint,
    getCurrentPower:          OneToOnePlanningVariationPoint,
    calculatePowerRequired:   OneToOnePlanningVariationPoint,
    extractPower:            OneToPairPlanningVariationPoint
  ): Int = {
    implicit val dynamicPlanner = planner
    val defaults = Seq(
      "ExtractedOutsideTempF" =:= false,
      "ExtractedInsideTempF" =:= false,
      "ExpectedDemandF" =:= false,
      "ExtractedOutsideTempC" =:= false,
      "ExtractedInsideTempC" =:= false,
      "ExpectedDemandC" =:= false,
      "ExtractedCurrentPowerW" =:= false,
      "PowerRequired" =:= false
    )

    val initialDB = buildings.initialConditions(defaults: _*)
    val withOutsideTemp = initialDB.plannedMap(f => getOutsideTemp(f), getOutsideTemp)
    val withInsideTemp = withOutsideTemp.plannedMap(f => getInsideTemp(f), getInsideTemp)
    val withDemand = withInsideTemp.plannedMap(f => calculateExpectedDemand(f), calculateExpectedDemand)
    val withCurrentPower = withDemand.plannedMap(f => getCurrentPower(f), getCurrentPower)
    val withPowerRequired = withCurrentPower.plannedMap(f => calculatePowerRequired(f), calculatePowerRequired)
    val extracted = withPowerRequired.plannedMap(f => extractPower(f), extractPower)

    extracted.goalConditions("PowerRequired" =:= true)
    val result = extracted.plannedCollect[(Long, Option[Watts])]()

    result.length
  }

  def update(planner: DynamicPlanner): Unit = {
    val jobId = planner.getLastJobId.get
    planner.registerModule(ExpectedDemandFromC)
    planner.removeModule(ExpectedDemandFromF.getName)
    planner.replanPipeline(jobId)
  }
}
