package geo.utils

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import geo.Params
import org.apache.log4j.Logger
import org.datasyslab.geospark.spatialRDD.{LineStringRDD, PointRDD}

import scala.util.Random

object Utils {
  val resultsLogger = Logger.getLogger("results")
  val reportLogger = Logger.getLogger("reports")

  lazy val stats = new SimpleStats(RunContext.headerCsv)

  /** runs an action and logs its runtime */
  def logTime[R](actionName: String)(block: => R)(implicit context: RunContext): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    val elapsedSeconds = (t1 - t0) / 1000.0
    resultsLogger.info(f"${context.keyCsv},${context.iteration},$elapsedSeconds%10.3f,$actionName")
    stats.collect(s"${context.keyCsv},$actionName", elapsedSeconds)
    result
  }

  def logStatisticsReport(): Unit = {
    reportLogger.info(stats.report)
  }

  def benchmarkRun[T](params: Params, runName: String, times: Int = 5)(action: RunContext => T): Unit = {
    for (i <- 1 to times) {
      implicit val context = RunContext(params, runName, i)
      Utils.logTime(s"z.---TOTAL") {
        action(context)
      }
    }
  }

  /**
    * An inaccurate conversion of meters to latitude (good enough for our benchmarking purposes)
    *
    *   at Vavel location:
    *   - longitude 1m = 0.0000145
    *   - latitude 1m = 0.0000131
    *   roughly  1m ~ e-5
    */
  val meterLatitudeFactor = 1.0 / 100000

  /**
    * Shifts coordinates (using Gaussian distribution), so that 95% are within confInterval95pct of the original points
    *
    * @param coordinate        the coordinate to be shifted
    * @param rnd               random number generator
    * @param confInterval95pct 95 pct confidence interval for the shifted point location
    * @return a new, shifted [[Coordinate]]
    */
  def shiftCoordinateMetersGaussian(coordinate: Coordinate, rnd: Random, confInterval95pct: Double = 1.96): Coordinate = {
    val confidenceScaleFactor = confInterval95pct / 1.96
    new Coordinate(
      coordinate.x + rnd.nextGaussian() * confidenceScaleFactor * meterLatitudeFactor,
      coordinate.y + rnd.nextGaussian() * confidenceScaleFactor * meterLatitudeFactor,  //  yes, it's longitude, but it's good enough for our testing purposes...
      coordinate.z)
  }

  /**
    * Generates an rdd of points randomly spaced around a sample of the roads from roadsRdd
    *
    * @param roadsRdd        RDD of line strings, representing roads
    * @param sampleCount     how many sample points to generate
    * @param perRoadCount    how many points per single road to generate
    * @param geometryFactory a geometry factory
    * @param seed            random seeed for repeatability
    * @return rdd of points
    */
  def generateSampleGPSPointRdd(roadsRdd: LineStringRDD, sampleCount: Int = 1000, perRoadCount: Int = 1, geometryFactory: GeometryFactory, seed: Int): PointRDD = {
    val roadCount = roadsRdd.rawSpatialRDD.count
    new PointRDD(
      roadsRdd.rawSpatialRDD.rdd
        .sample(withReplacement = false, fraction = (1.0 * sampleCount / perRoadCount) / roadCount, seed = 13)
        .flatMap(x => x.getCoordinates.take(perRoadCount))
        .mapPartitions(l => {
          val rnd = new Random(seed)
          l.map(coord => Utils.shiftCoordinateMetersGaussian(coord, rnd))
        })
        .map(x => geometryFactory.createPoint(x))
        .cache()
    )
  }

}
