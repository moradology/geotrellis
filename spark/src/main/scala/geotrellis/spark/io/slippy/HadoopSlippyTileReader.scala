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
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable
import scala.reflect.ClassTag

class HadoopSlippyTileReader[T: ClassTag](uri: String)(fromBytes: (SpatialKey, Array[Byte]) => T)(implicit sc: SparkContext) extends SlippyTileReader[T] {

  private val uriRoot = new Path(uri)
  private val hadoopConfiguration = sc.hadoopConfiguration

  def read(zoom: Int, key: SpatialKey): T = {
    import SlippyTileReader.TilePath
    val fs = uriRoot.getFileSystem(hadoopConfiguration)
    val hadoopPath = uriRoot.suffix("/$zoom/${key.col}/${key.row}")

    if (fs.isFile(hadoopPath)) { fromBytes(key, IOUtils.toByteArray(fs.open(hadoopPath))) }
    else { sys.error(s"No tile located at ${hadoopPath.toString}") }
  }

  def read(zoom: Int): RDD[(SpatialKey, T)] = {
    import SlippyTileReader.TilePath
    val fs = uriRoot.getFileSystem(hadoopConfiguration)

    val paths = mutable.ArrayBuffer.empty[(SpatialKey, Path)]
    val fileIter = fs.listFiles(uriRoot, true)
    while (fileIter.hasNext) {
      val path = fileIter.next.getPath
      path.toString match {
        case TilePath(z, x, y) if z.toInt == zoom =>
          paths.append((SpatialKey(x.toInt, y.toInt), path))
        case _ => ()
      }
    }
    val numPartitions = math.min(paths.size, math.max(paths.size / 10, 50)).toInt
    val tiles = paths.map { case (spatialKey, hadoopPath) =>
      (spatialKey, fromBytes(spatialKey, IOUtils.toByteArray(fs.open(hadoopPath))))
    }
    sc.parallelize(tiles)
      .partitionBy(new HashPartitioner(numPartitions))

      // It's unclear to me what the performance penalties might be for not using this code;
      // unclear how to read from HDFS on this side
      /**.mapPartitions({ partition =>
        partition

        //partition.map { case (spatialKey, hadoopPath) =>
        //  (spatialKey, fromBytes(spatialKey, toByteArray(fs.open(hadoopPath))))
        //}

      }, preservesPartitioning = true)
    **/
  }
}

