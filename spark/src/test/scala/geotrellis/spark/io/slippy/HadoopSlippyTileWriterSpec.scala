package geotrellis.spark.io.slippy

import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.proj4._
import geotrellis.vector._

import geotrellis.spark.testfiles._

import org.apache.hadoop.fs.Path

import org.scalatest._
import java.io.File

class HadoopSlippyTileWriterSpec
    extends FunSpec
    with Matchers
    with TestEnvironment
    with TestFiles
    with RasterRDDMatchers
    with OnlyIfCanRunSpark {
  describe("HadoopSlippyTileWriter") {
    val testPath = new Path(new Path(outputLocalPath.getPath), "slippy-write-test")

    ifCanRunSpark {
      it("can write and read slippy tiles") {
        val mapTransform = ZoomedLayoutScheme(WebMercator).levelForZoom(TestFiles.ZOOM_LEVEL).layout.mapTransform

      val writer =
        new HadoopSlippyTileWriter[Tile](testPath, "tif")({ (key, tile) =>
          SingleBandGeoTiff(tile, mapTransform(key), WebMercator).toByteArray
        })

      val reader =
        new HadoopSlippyTileReader[Tile](testPath)({ (key, bytes) =>
          SingleBandGeoTiff(bytes).tile
        })

        writer.write(TestFiles.ZOOM_LEVEL, AllOnesTestFile)
        rastersEqual(reader.read(TestFiles.ZOOM_LEVEL), AllOnesTestFile)
      }

    }
  }
}
