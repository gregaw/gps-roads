package geo


import java.io.File

import com.vividsolutions.jts.geom.{GeometryFactory, LineString, Point}
import geo.utils.Utils
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.spatialRDD.{LineStringRDD, PointRDD}

import scala.util.Random

object PrepareData extends App {

  //  define your needs here:

  val originalRoadsName = "pl-mlp-roads"
  val originalPointsCount = 265000
  val samplePointsCount = 100000
  val sampleRoadsCount = 100000
  
  val conf = new SparkConf().setAppName("basic").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val seed = 13
  val geometryFactory = new GeometryFactory()

  val sampleRoadsName = s"$originalRoadsName-${sampleRoadsCount/1000}k"

  private val originalRoadsShapefile = s"/tmp/data/$originalRoadsName"
  private val roadsFolder = s"/tmp/data/$sampleRoadsName"
  private val pointsFolder = s"/tmp/data/points-${samplePointsCount/1000}k.from.$sampleRoadsName"


  if (!new File(roadsFolder).exists())
    createRoadsSample(sc, originalRoadsShapefile, roadsFolder, fraction = 1.0 * sampleRoadsCount / originalPointsCount)
  else
    println(s"Skipped creating roads sample. It already exists under $roadsFolder")


  if (!new File(pointsFolder).exists())
    createPointsSample(sc, roadsFolder, pointsFolder, samplePointsCount)
  else
    println(s"Skipped creating points sample. It already exists under $pointsFolder")


  val roadsCount = new LineStringRDD(sc.objectFile[LineString](roadsFolder)).countWithoutDuplicates()
  val pointsCount = new PointRDD(sc.objectFile[Point](pointsFolder)).countWithoutDuplicates()

  sc.stop()

  println(s"Roads: $roadsCount, Points: $pointsCount")

  println("Done.")

  //  ======================================================

  private def createPointsSample(sc: SparkContext, roadsInputFolder: String, outputFolder: String, sampleCount: Int, perRoadCount: Int = 3) = {
    val graphRdd = new LineStringRDD(sc.objectFile[LineString](roadsInputFolder))
    graphRdd.analyze()

    val samplePointRdd = Utils.generateSampleGPSPointRdd(graphRdd, sampleCount, perRoadCount, geometryFactory, seed)
    samplePointRdd.rawSpatialRDD.saveAsObjectFile(outputFolder)
  }

  private def createRoadsSample(sc: SparkContext, inputFolder: String, outputFolder: String, fraction: Double) = {
    val graphRdd = ShapefileReader.readToLineStringRDD(sc, inputFolder)
    graphRdd.analyze()
    graphRdd.rawSpatialRDD.sample(withReplacement = false, fraction).saveAsObjectFile(outputFolder)
  }

}
