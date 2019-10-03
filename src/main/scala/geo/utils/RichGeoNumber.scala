package geo.utils

class RichGeoNumber(number: Double) {

  def meters: Double = Utils.meterLatitudeFactor * number

  def m: Double = meters

  def meter: Double = meters

  def km: Double = meters * 1000

  def toMeters: Double = number / Utils.meterLatitudeFactor

  def toKilometers: Double = toMeters * 1000
}
