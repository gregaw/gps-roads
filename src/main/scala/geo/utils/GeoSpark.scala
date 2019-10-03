package geo.utils

import com.vividsolutions.jts.geom.Coordinate

object GeoSpark{

  object Implicits {
    implicit def toRichCoordinate(value: Coordinate): RichCoordinate = new RichCoordinate(value)

    implicit def toRichNumber(value: Double): RichGeoNumber = new RichGeoNumber(value)

    implicit def toRichString(value: String): RichString = new RichString(value)
  }

}