package geo.utils

import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PointRDD}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConversions._

class GeoSparkSpecWrapper extends FlatSpec with Matchers with GeoSparkTestWrapper {

  import geo.utils.GeoSpark.Implicits._

  val simpleRoadRdd = rddFromLineStrings(
    lineStringFromCoords("empty-userData", vavel.north(100.meters), vavel, vavel.east(100.meters))
  )
  simpleRoadRdd.analyze()

  "A simple vavel road" should " intersect with all required points just fine " in {

    val intersecting = List(
      vavel,
      vavel.south(0.999.meter)
    ).map(geometryFactory.createPoint)

    val notIntersecting = List(
      vavel.south(2.meters),
      vavel.south(1.1.meter),
      vavel.west(1.1.meter),
      vavel.east(2.meter).north(2.meter),
      vavel.east(102.meters)
    ).map(geometryFactory.createPoint)

    val roadPoints = intersecting ++ notIntersecting

    val pointsRdd = new PointRDD(sc.parallelize(roadPoints))
    val circleRdd = new CircleRDD(pointsRdd, 1.meter)
    circleRdd.analyze()

    circleRdd.spatialPartitioning(GridType.KDBTREE)
    simpleRoadRdd.spatialPartitioning(circleRdd.getPartitioner)

    val result = JoinQuery.DistanceJoinQuery(
      simpleRoadRdd,
      circleRdd,
      false,
      true)
      .collect()

    assert(result.map(x => x._1) === intersecting)

  }

}
