package rugds.experiments.scenarios.simple

case class BuildingData(buildingId: Long, data: Map[String, Any])

case class Watts(v: Double)
case class Celsius(v: Double)
case class Fahrenheit(v: Double)