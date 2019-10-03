package geo.utils

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, LineString}
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.spatialRDD.LineStringRDD

trait GeoSparkTestWrapper {
  val geometryFactory = new GeometryFactory()

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[1]")
      .appName("testSpark")
      .getOrCreate()
  }
  lazy val sc = spark.sparkContext

  val vavel: Coordinate = new Coordinate(50.0540, 19.9354)

  def lineStringFromCoords(userData: String, coordinates: Coordinate*): LineString = {
    val value = geometryFactory.createLineString(coordinates.toArray)
    value.setUserData(userData)
    value
  }

  def rddFromLineStrings(lineString: LineString*): LineStringRDD = {
    new LineStringRDD(sc.parallelize(lineString.toSeq))
  }

}
