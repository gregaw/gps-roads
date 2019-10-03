import sbt.Keys.{libraryDependencies, version}


lazy val root = (project in file(".")).
  settings(
    name := "geospark-show",

    version := "0.1.0",

    scalaVersion := "2.11.11",

    organization := "com.virtuslab",

    publishMavenStyle := true
  )
val SparkVersion = "2.4.0"

val SparkCompatibleVersion = "2.3"

val HadoopVersion = "2.7.2"

val GeoSparkVersion = "1.2.0"


val dependencyScope = "compile"

logLevel := Level.Info

logLevel in assembly := Level.Info
mainClass in assembly := Some("geo.Benchmark")
assemblyJarName in assembly := "../geoBenchmark.jar"
test in assembly := {}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SparkVersion % dependencyScope exclude("org.apache.hadoop", "*"),
  "org.apache.spark" %% "spark-sql" % SparkVersion % dependencyScope exclude("org.apache.hadoop", "*"),
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % HadoopVersion % dependencyScope,
  "org.apache.hadoop" % "hadoop-common" % HadoopVersion % dependencyScope,
  "org.datasyslab" % "geospark" % GeoSparkVersion,
  "org.datasyslab" % "geospark-sql_".concat(SparkCompatibleVersion) % GeoSparkVersion ,
  "org.datasyslab" % "geospark-viz_".concat(SparkCompatibleVersion) % GeoSparkVersion
)

assemblyMergeStrategy in assembly := {
  case PathList("org.datasyslab", "geospark", xs@_*) => MergeStrategy.first
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case path if path.endsWith(".SF") => MergeStrategy.discard
  case path if path.endsWith(".DSA") => MergeStrategy.discard
  case path if path.endsWith(".RSA") => MergeStrategy.discard
  case _ => MergeStrategy.first
}

resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

resolvers +=
  "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools"

initialCommands in console := s"""
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PointRDD}
import org.apache.log4j.{Level, Logger}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
val conf = new SparkConf().setAppName("basic").setMaster("local[1]")
val sc = new SparkContext(conf)
  """

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

parallelExecution in Test := false