package geo.mesh

import geo.utils.GeoSparkTestWrapper
import org.scalatest.{FlatSpec, Matchers}

class MeshMappingSpec extends FlatSpec with Matchers with GeoSparkTestWrapper {

  import geo.utils.GeoSpark.Implicits._

  val vavelRoadName = "vavel"
  val vavelRoadId = 0
  val vavelRoad = lineStringFromCoords(vavelRoadName, vavel.north(50.meters), vavel, vavel.east(100.meters))
  val kopecRoadName = "kopec"
  val kopecRoadId = 1
  val kopecRoad = lineStringFromCoords(kopecRoadName, vavel.south(50.meters), vavel, vavel.west(100.meters))

  val simpleRoadRdd = rddFromLineStrings(vavelRoad, kopecRoad)

  simpleRoadRdd.analyze()

  val simpleRoadMesh: Mesh = Mesh.buildMesh(simpleRoadRdd, 1.meter, sc, geometryFactory)

  "Mesh map " should " be larger than the road graph boundary " in {
    assert(simpleRoadRdd.boundaryEnvelope.getMinX === simpleRoadMesh.config.envelope.getMinX + 1.meter)
    assert(simpleRoadRdd.boundaryEnvelope.getMinY === simpleRoadMesh.config.envelope.getMinY + 1.meter)
    assert(simpleRoadRdd.boundaryEnvelope.getMaxX === simpleRoadMesh.config.envelope.getMaxX - 1.meter)
    assert(simpleRoadRdd.boundaryEnvelope.getMaxY === simpleRoadMesh.config.envelope.getMaxY - 1.meter)
  }

  it should "show hits" in {
    assert(simpleRoadMesh.get(vavel) === Set(vavelRoadId, kopecRoadId))

    assert(simpleRoadMesh.get(vavel.east(0.25.meters).north(0.25.meters)) === Set(vavelRoadId, kopecRoadId))
    assert(simpleRoadMesh.get(vavel.east(100.25.meters)) === Set(vavelRoadId))

    assert(simpleRoadMesh.get(vavel.north(50.25.meters)) === Set(vavelRoadId))

    assert(simpleRoadMesh.get(vavel.south(0.25.meters)) === Set(vavelRoadId, kopecRoadId))
    assert(simpleRoadMesh.get(vavel.west(0.25.meters)) === Set(vavelRoadId, kopecRoadId))
  }

  it should "show misses" in {
    assert(simpleRoadMesh.get(vavel.east(102.meters)) === Set.empty)
    assert(simpleRoadMesh.get(vavel.east(2.meters).north(2.meters)) === Set.empty)
    assert(simpleRoadMesh.get(vavel.north(52.meters)) === Set.empty)
  }

  it should "persist properly" in {

    val path = "/tmp/test.zmes§h"

    Mesh.save(simpleRoadMesh,path)

    val meshToRead = Mesh.load(path)

    assert(simpleRoadMesh.stats == meshToRead.stats)
  }

}
