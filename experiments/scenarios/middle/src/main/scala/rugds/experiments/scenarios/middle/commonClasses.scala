package rugds.experiments.scenarios.middle

case class KMpH(v: Double) {
  def toMpS: MpS = MpS(v / 3600 * 1000)
}
case class MpS(v: Double) {
  def toKMpH: KMpH = KMpH(v / 1000 * 3600)
}

case class Meters(v: Double) {
  def toKM: Kilometers = Kilometers(v / 1000)
}
case class Kilometers(v: Double) {
  def toM: Meters = Meters(v * 1000)
}

case class VehicleData(vehicleId: Long, speedNow: KMpH, speedOther: KMpH, locationNow: Meters, locationOther: Meters)
case class AccidentData(accidentId: Long, locationAccident: Meters)

case class Pack(left: Option[Meters], right: Option[Meters])
