package geo.mesh

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, LineString, Point}
import geo.mesh.Mesh.{MeshKey, RoadId}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{CircleRDD, LineStringRDD, PointRDD}

import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.{immutable, mutable}
import scala.concurrent.forkjoin.ForkJoinPool


/**
  * Stores a set of [[LineString]] in its cells accessible by [[Coordinate]].
  *
  * @param config configuration, meta data
  * @param data   contents of the mesh
  */
case class Mesh(config: MeshConfig, data: Map[MeshKey, Set[RoadId]], roadIds: Map[RoadId, String]) {

  val empty = Set.empty[RoadId]

  /** maps to the coordinate */
  def get(coord: Coordinate): Set[RoadId] = {
    data.getOrElse(config.coordinateToKey(coord), empty)
  }

  def size: Int = data.size

  def stats: String = {

    val buffer = mutable.Map[Int, Int]().withDefaultValue(0)

    data
      .map { case (_, set) => set.size }
      .foreach { key =>
        buffer += key -> (buffer(key) + 1)
      }


    s"count_distinct=${buffer.size}, pointsCount->roadsMapped=${buffer.toList.sorted.mkString(",")}"
  }

}

object Mesh {

  type MeshKey = Long
  type RoadId = Int

  import scala.collection.JavaConversions._

  val logger = Logger.getLogger(Mesh.getClass)

  /**
    * Builds an instance of [[geo.mesh.Mesh]] by generating all the mesh points and joining with the road graph
    *
    * @param lineStringRdd   a set of roads
    * @param radius          defines the catchment area of a GPS point
    * @param sc              spark context
    * @param geometryFactory is used to create [[com.vividsolutions.jts.geom.Point]]
    * @return a newly built Mesh
    */
  def buildMeshBigBang(lineStringRdd: LineStringRDD, radius: Double, sc: SparkContext, geometryFactory: GeometryFactory): Mesh = {
    val config = MeshConfig(lineStringRdd.boundaryEnvelope, radius)

    val roadIds = extractRoadIds(lineStringRdd)

    val pointsRdd = new PointRDD(sc.parallelize(config.getAllMeshPointCoordinates.toSeq.map(geometryFactory.createPoint), numSlices = 1000))
    val circleRdd = new CircleRDD(pointsRdd, radius)
    circleRdd.analyze()

    circleRdd.spatialPartitioning(GridType.KDBTREE)
    lineStringRdd.spatialPartitioning(circleRdd.getPartitioner)

    lineStringRdd.buildIndex(IndexType.QUADTREE, true)

    val useIndex = true
    val considerBoundaryIntersection = true

    val result = JoinQuery.DistanceJoinQuery(lineStringRdd, circleRdd, useIndex, considerBoundaryIntersection)
      .collect
      .toList
      .map { case (point: Point, roads) => (config.coordinateToKey(point.getCoordinate), roads.toSet.map((x: LineString) => roadIds(x.getUserData.toString))) }
      .toMap

    val reversedRoadIds = roadIds.map { case (x, y) => (y, x) }.withDefaultValue("unknown")

    new Mesh(config, result, reversedRoadIds)

  }

  /**
    * Builds an instance of [[geo.mesh.Mesh]] by batch iterating over the mesh of points and joining with the road graph
    *
    * @param lineStringRdd   a set of roads
    * @param radius          defines the catchment area of a GPS point
    * @param sc              spark context
    * @param geometryFactory is used to create [[com.vividsolutions.jts.geom.Point]]
    * @return a newly built Mesh
    */
  def buildMesh(lineStringRdd: LineStringRDD, radius: Double, sc: SparkContext, geometryFactory: GeometryFactory): Mesh = {
    val config = MeshConfig(lineStringRdd.boundaryEnvelope, radius)

    lineStringRdd.spatialPartitioning(GridType.QUADTREE)
    lineStringRdd.buildIndex(IndexType.QUADTREE, true)

    val roadIds = extractRoadIds(lineStringRdd)
    val batchSize = 500000
    val batchedCoordinates = config.getAllMeshPointCoordinates.grouped(batchSize)

    def mapPoints(batch: Seq[Coordinate]) = {
      val pointsRdd = new PointRDD(sc.parallelize(batch.map(geometryFactory.createPoint)))
      val circleRdd = new CircleRDD(pointsRdd, radius)
      circleRdd.spatialPartitioning(lineStringRdd.getPartitioner)
      circleRdd.analyze()

      val useIndex = true
      val considerBoundaryIntersection = true

      JoinQuery.DistanceJoinQuery(lineStringRdd, circleRdd, useIndex, considerBoundaryIntersection)
        .collect
        .toList
        .map {
          case (point: Point, roads) => (
            config.coordinateToKey(point.getCoordinate),
            roads.toSet.map((x: LineString) => roadIds(x.getUserData.toString))
          )
        }
        .toMap
    }

    val batchResults = for {
      batch <- batchedCoordinates
      mapped <- mapPoints(batch)
      if mapped._2.nonEmpty
    }
      yield mapped

    val reversedRoadIds = roadIds.map { case (x, y) => (y, x) }.withDefaultValue("unknown")
    new Mesh(config, batchResults.toMap, reversedRoadIds)
  }

  private def extractRoadIds(lineStringRdd: LineStringRDD) = {
    lineStringRdd.rawSpatialRDD.rdd.map(_.getUserData.toString).distinct().collect().zipWithIndex.toMap.withDefaultValue(-1)
  }

  def load(path: String): Mesh = {

    val in = new ObjectInputStream(new GZIPInputStream(new FileInputStream(path)))
    val mesh = in.readObject().asInstanceOf[Mesh]
    in.close()

    mesh
  }

  def save(mesh: Mesh, path: String): Unit = {
    val out = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(path)))
    out.writeObject(mesh)
    out.close()
  }

}