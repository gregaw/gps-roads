package geo.utils

import com.vividsolutions.jts.geom.Coordinate

/**
  * rough implementation; test purposes only
  */
class RichCoordinate(coord: Coordinate){
  def shift(x: Double=0, y: Double=0): Coordinate ={
    new Coordinate(coord.x+x, coord.y+y, coord.z)
  }

  def east(value: Double): Coordinate = shift(x = value)
  def west(value: Double): Coordinate = shift(x = -value)
  def north(value: Double): Coordinate = shift(y = value)
  def south(value: Double): Coordinate = shift(y = -value)

}
