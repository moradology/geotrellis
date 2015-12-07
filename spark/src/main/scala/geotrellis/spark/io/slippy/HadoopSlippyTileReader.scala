package geotrellis.spark.io.slippy

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.hadoop.formats._

import org.apache.commons.io.IOUtils
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.input.PortableDataStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable
import scala.reflect.ClassTag

class HadoopSlippyTileReader[T: ClassTag](uri: Path)(fromBytes: (SpatialKey, Array[Byte]) => T)(implicit sc: SparkContext) extends SlippyTileReader[T] {

  def read(zoom: Int, key: SpatialKey): T = {
    import SlippyTileReader.TilePath
    val fs = uri.getFileSystem(sc.hadoopConfiguration)
    val hadoopPath = new Path(uri, "$zoom/${key.col}/${key.row}")

    if (fs.isFile(hadoopPath)) { fromBytes(key, IOUtils.toByteArray(fs.open(hadoopPath))) }
    else { sys.error(s"No tile located at ${hadoopPath.toString}") }
  }

  def read(zoom: Int): RDD[(SpatialKey, T)] = {
    import SlippyTileReader.TilePath

    val lUri = new Path(uri, s"$zoom/*/*")
    val lFromBytes = fromBytes

    val binFiles: RDD[(String, PortableDataStream)] = sc.binaryFiles(lUri.toString)
    binFiles.collect { case (path: String, pds: PortableDataStream) =>
      val spatialKey = path match { case TilePath(z, x, y) if z.toInt == zoom => SpatialKey(x.toInt, y.toInt) }
      (spatialKey, lFromBytes(spatialKey, pds.toArray))
    }
  }
}

