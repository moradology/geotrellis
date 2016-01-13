package geotrellis.raster

import geotrellis.vector.Extent
import geotrellis.raster.resample._

import spire.syntax.cfor._
import java.nio.ByteBuffer

/**
 * ArrayTile based on Array[Byte] (each cell as a Byte).
 */
final case class RawByteArrayTile(array: Array[Byte], cols: Int, rows: Int)
  extends MutableArrayTile with IntBasedArrayTile {

  val cellType = ByteCellType

  def apply(i: Int) = array(i).toInt
  def update(i: Int, z: Int) = { array(i) = z.toByte }

  def toBytes: Array[Byte] = array.clone

  def copy = ArrayTile(array.clone, cols, rows)
}

object RawByteArrayTile {
  def ofDim(cols: Int, rows: Int): RawByteArrayTile =
    new RawByteArrayTile(Array.ofDim[Byte](cols * rows), cols, rows)

  def empty(cols: Int, rows: Int): RawByteArrayTile =
    new RawByteArrayTile(Array.ofDim[Byte](cols * rows).fill(byteNODATA), cols, rows)

  def fill(v: Byte, cols: Int, rows: Int): RawByteArrayTile =
    new RawByteArrayTile(Array.ofDim[Byte](cols * rows).fill(v), cols, rows)

  def fromBytes(bytes: Array[Byte], cols: Int, rows: Int): RawByteArrayTile =
    RawByteArrayTile(bytes.clone, cols, rows)

  def fromBytes(bytes: Array[Byte], cols: Int, rows: Int, replaceNoData: Byte): RawByteArrayTile = {
    println("WARNING: Raw celltypes lack a NoData value; any default value will be ignored")
    fromBytes(bytes, cols, rows)
  }
}