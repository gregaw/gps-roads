package geo

import java.nio.file.{Files, Paths}

import com.vividsolutions.jts.geom.{GeometryFactory, Point}
import geo.mesh.Mesh
import geo.utils.{RunContext, Utils}
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{CircleRDD, LineStringRDD, PointRDD}

import scala.reflect.io.File

case class Params(
                   testName: String,
                   roadCount: Int,
                   pointCount: Int,
                   withIndex: Boolean,
                   mesh: Boolean,
                   meshIntervalMeters: Int,
                   cores: Int,
                   roadPartitions: Int,
                   indexType: IndexType,
                   spatialPartitioning: GridType
                 ) {
  val runSummaryCsv: String = s"$testName," +
    s"${1.0 * roadCount / 1000}," +
    s"${1.0 * pointCount / 1000}," +
    s"$withIndex," +
    s"$mesh," +
    s"$meshIntervalMeters," +
    s"$cores," +
    s"$roadPartitions," +
    s"$indexType," +
    s"$spatialPartitioning"
}

object Params {
  lazy val runSummaryHeaderCsv: String = "testName,roadCount,pointCount,withIndex,mesh,meshIntervalMeters,cores,roadPartitions,indexType,spatialPartitioning"
}

object Benchmark extends App {

  type OptionMap = Map[Symbol, Any]

  val seed = 13

  val dataFolder = "/Users/gregaw/tmp/data"

  val logger = Logger.getLogger(Benchmark.getClass)

  import geo.utils.GeoSpark.Implicits._

  val parameters: Params = parseParameters(args, Params(
    testName = "defaultTest",
    roadCount = 200,
    pointCount = 100,
    withIndex = false,
    mesh = false,
    meshIntervalMeters = 1000,
    cores = 1,
    roadPartitions = 1,
    indexType = IndexType.RTREE,
    spatialPartitioning = GridType.RTREE
  )
  )

  val resultsLogger = Utils.resultsLogger

  val conf = new SparkConf().setAppName("basic").setMaster(s"local[${parameters.cores}]")
  val sc = new SparkContext(conf)

  val geometryFactory = new GeometryFactory()

  private val regionName = "pl-mlp-roads"
  private val regionFolder = s"$dataFolder/$regionName"
  val shapeFileRdd = ShapefileReader.readToLineStringRDD(sc, regionFolder)
  val totalRoadCount = shapeFileRdd.rawSpatialRDD.count()
  private val roadsRddPartitioned =
    if (parameters.roadCount > 0) {
      shapeFileRdd
        .rawSpatialRDD
        .sample(withReplacement = false, 1.0 * parameters.roadCount / totalRoadCount, seed)
        .repartition(parameters.roadPartitions)
        .cache()
    } else {
      shapeFileRdd
        .rawSpatialRDD
        .repartition(parameters.roadPartitions)
        .cache()
    }
  val graphRdd = new LineStringRDD(roadsRddPartitioned)
  graphRdd.analyze()

  val samplePointRdd = Utils.generateSampleGPSPointRdd(shapeFileRdd, parameters.pointCount, perRoadCount = 3, geometryFactory, seed)
  samplePointRdd.analyze()

  val times = 5

  if (parameters.mesh) {

    logger.info("creating mesh...")
    //    Utils.benchmarkRun(parameters, "create mesh", times) { _ =>
    //      Mesh.buildMesh(graphRdd, 10.meters, sc, geometryFactory)
    //    }
    val meshPath = s"$dataFolder/region=$regionName.samples=${parameters.roadCount}.interval=${parameters.meshIntervalMeters}m.zmesh"

    Utils.benchmarkRun(parameters, "mesh-WARMUP", 1) { context =>
      benchmarkMesh(meshPath)(context)
    }

    Utils.benchmarkRun(parameters, "mesh", times) { context =>
      benchmarkMesh(meshPath)(context)
    }

  } else {

    //  first run is to collect the data details
    Utils.benchmarkRun(parameters, "geospark-WARMUP", 1) { context =>
      benchmarkDistanceJoinQuery(graphRdd, samplePointRdd, parameters, verbose = true)(context)
    }

    Utils.benchmarkRun(parameters, "geospark", times) { context =>
      benchmarkDistanceJoinQuery(graphRdd, samplePointRdd, parameters)(context)
    }

  }

  private def benchmarkMesh(meshPath: String)(implicit runContext: RunContext) = {
    Utils.logTime("a.create mesh") {

      if (!Files.exists(Paths.get(meshPath))) {
        logger.warn(s"Mesh file not found; recreating in: $meshPath")
        val mesh = Mesh.buildMesh(graphRdd, parameters.meshIntervalMeters.meters, sc, geometryFactory)
        logger.info(s"mesh size: ${mesh.size} and stats: ${mesh.stats}")
        Mesh.save(mesh, meshPath)
      }
    }

    val mesh = Utils.logTime("b.load mesh") {
      logger.info(s"loading mesh file from: $meshPath")
      Mesh.load(meshPath)
    }

    val points = Utils.logTime("c.prepare points") {
      import scala.collection.JavaConversions._
      samplePointRdd.rawSpatialRDD.collect().toVector
    }

    Utils.logTime("z.LOOKUP") {
      for (p: Point <- points) {
        mesh.get(p.getCoordinate)
      }
    }
  }

  Utils.logStatisticsReport()

  sc.stop()

  println("Done.")


  private def benchmarkDistanceJoinQuery(graphRdd: LineStringRDD, pointsRdd: PointRDD, parameters: Params, verbose: Boolean = false)
                                        (implicit context: RunContext) {

    val pointRdd = new CircleRDD(pointsRdd, parameters.meshIntervalMeters.meters)
    pointRdd.analyze()
    graphRdd.analyze()

    Utils.logTime("a.point spatial partitioning") {
      pointRdd.spatialPartitioning(parameters.spatialPartitioning)
    }
    Utils.logTime("b.graph spatial partitioning") {
      graphRdd.spatialPartitioning(pointRdd.getPartitioner)
    }
    Utils.logTime("c.creating index") {
      if (parameters.withIndex) {
        graphRdd.buildIndex(parameters.indexType, true)
      }
    }

    Utils.logTime("d.point persist") {
      pointRdd.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
    }

    Utils.logTime("e.graph persist") {
      graphRdd.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
    }

    if (verbose) {
      resultsLogger.info(s"details,counts in points: ${pointRdd.rawSpatialRDD.count()}")
      resultsLogger.info(s"details,counts in graph: ${graphRdd.rawSpatialRDD.count()}")
    }

    Utils.logTime("z.LOOKUP") {
      val considerBoundaryIntersection = true
      val joinCount = JoinQuery.DistanceJoinQuery(graphRdd, pointRdd, parameters.withIndex, considerBoundaryIntersection).count()
      if (verbose)
        resultsLogger.info(s"details,counts in join: $joinCount")
    }
  }

  private def parseIndexType(value: String) = value.toUpperCase() match {
    case "RTREE" => IndexType.RTREE
    case "QUADTREE" => IndexType.QUADTREE
  }

  def parseSpatialPartitioning(value: String): GridType = value.toUpperCase() match {
    case "RTREE" => GridType.RTREE
    case "QUADTREE" => GridType.QUADTREE
    case "KDBTREE" => GridType.KDBTREE
  }

  private def parseParameters(args: Array[String], defaults: Params): Params = {
    if (args.length == 0) {
      logger.warn(s"No arguments found on the command line, using defaults: $defaults")
      defaults
    } else {

      def recurseOptions(current: Params, list: List[String]): Params = {

        list match {
          case Nil => current
          case "--testName" :: value :: tail =>
            recurseOptions(current.copy(testName = value), tail)
          case "--cores" :: value :: tail =>
            recurseOptions(current.copy(cores = value.toInt), tail)

          case "--roadsCount" :: value :: tail =>
            recurseOptions(current.copy(roadCount = value.toInt), tail)
          case "--pointsCount" :: value :: tail =>
            recurseOptions(current.copy(pointCount = value.toInt), tail)


          case "--mesh" :: value :: tail =>
            recurseOptions(current.copy(mesh = value.toBoolean), tail)
          case "--meshIntervalMeters" :: value :: tail =>
            recurseOptions(current.copy(meshIntervalMeters = value.toInt), tail)


          case "--withIndex" :: value :: tail =>
            recurseOptions(current.copy(withIndex = value.toBoolean), tail)
          case "--roadPartitions" :: value :: tail =>
            recurseOptions(current.copy(roadPartitions = value.toInt), tail)
          case "--indexType" :: value :: tail =>
            recurseOptions(current.copy(indexType = parseIndexType(value)), tail)
          case "--spatialPartitioning" :: value :: tail =>
            recurseOptions(current.copy(spatialPartitioning = parseSpatialPartitioning(value)), tail)

          case option :: _ => logger.error("Unknown option " + option)
            throw new IllegalArgumentException(s"Unknown option: $option")
        }
      }

      val params = recurseOptions(defaults, args.toList)
      logger.info(s"Using the following params: $params")
      params
    }
  }


}
