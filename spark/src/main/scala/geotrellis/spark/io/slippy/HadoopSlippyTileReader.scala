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
import org.apache.hadoop.fs.{Path, FileSystem}

import scala.collection.JavaConversions._
import scala.collection.mutable

class HadoopSlippyTileReader[T](uri: String, extension: String)(fromBytes: (SpatialKey, Array[Byte]) => T)(implicit sc: SparkContext) extends SlippyTileReader[T] {
  import SlippyTileReader.TilePath

  val uriRoot = new Path(uri)

  def read(zoom: Int, key: SpatialKey): T = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val hadoopPath = uriRoot.suffix("/$zoom/${key.col}/${key.row}")

    if (fs.isFile(hadoopPath)) { fromBytes(key, IOUtils.toByteArray(fs.open(hadoopPath))) }
    else { sys.error(s"No tile located at ${hadoopPath.toString}") }
  }

  def read(zoom: Int)(implicit sc: SparkContext): RDD[(SpatialKey, T)] = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val cs = fs.getContentSummary(uriRoot)
    val fileIter = fs.listFiles(uriRoot, true)

    val paths = mutable.ArrayBuffer.empty[(SpatialKey, Path)]
    while (fileIter.hasNext) {
      val path = fileIter.next.getPath
      path.toString match {
        case TilePath(z, x, y) if z.toInt == zoom => paths.append((SpatialKey(x.toInt, y.toInt), path))
        case _ => ()
      }
    }

    val numPartitions = math.min(paths.size, math.max(paths.size / 10, 50)).toInt
    sc.parallelize(paths)
      .partitionBy(new HashPartitioner(numPartitions))
      .mapPartitions({ partition =>
        val fs = FileSystem.get(sc.hadoopConfiguration)

        partition.map { case (spatialKey, hadoopPath) =>
          (spatialKey, fromBytes(spatialKey, IOUtils.toByteArray(fs.open(hadoopPath))))
        }
      }, preservesPartitioning = true)
  }
}

