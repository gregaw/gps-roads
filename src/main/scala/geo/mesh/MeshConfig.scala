package geo.mesh

import com.vividsolutions.jts.geom.{Coordinate, Envelope, LineString}

import scala.collection.immutable

/**
  * Contains configuration of a Mesh and provides the necessary conversions.
  *
  * @param originalEnvelope the envelope encompassing the input data
  * @param  interval the width/height of a single cell
  */
case class MeshConfig(originalEnvelope: Envelope, interval: Double) {

  /** original envelope expanded appropriately */
  lazy val envelope = {
    val meshEnvelope = new Envelope(originalEnvelope)
    meshEnvelope.expandBy(interval)
    meshEnvelope
  }

  /** index-size horizontal / Longitude */
  val height = math.ceil(envelope.getHeight / interval).toInt + 1

  /** index-size vertical / Latitude */
  val width = math.ceil(envelope.getWidth / interval).toInt + 1

  /** singleton to help with potentially large number of empty cell contents */
  val empty = Set.empty[LineString]

  /**
    * Converts coordinates to Mesh indices.
    *
    * @param coord proper geo-spatial coordinates
    * @return tuple of two indices in the mesh where the coordinate falls
    */
  def coordinate2MeshIndices(coord: Coordinate): (Int, Int) = (
    math.round((coord.x - envelope.getMinX) / interval).toInt,
    math.round((coord.y - envelope.getMinY) / interval).toInt
  )

  def coordinateToKey(coord: Coordinate): Long = {
    val (x,y) = coordinate2MeshIndices(coord)

    (x.toLong << 32) + y
  }

  /** the reverse of coordinate to indices */
  def meshIndices2Coordinate(x: Int, y: Int) =
    new Coordinate(
      envelope.getMinX + x * interval,
      envelope.getMinY + y * interval)

  /** maps any point within the originalEnvelope to the mesh points */
  def truncateCoordinateToMeshGrid(coord: Coordinate): Coordinate = {
    val indices = coordinate2MeshIndices(coord)
    meshIndices2Coordinate(indices._1, indices._2)
  }

  def getAllMeshPointCoordinates: Iterator[Coordinate] =
    for {
      x <- (0 until width).iterator
      y <- (0 until height).iterator
    }
      yield meshIndices2Coordinate(x, y)

  def pointCount: Int = (width+1) * (height+1)

}
