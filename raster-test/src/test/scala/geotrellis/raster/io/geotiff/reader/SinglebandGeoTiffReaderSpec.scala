package geotrellis.raster.io.geotiff.reader

import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.testkit._

import spire.syntax.cfor._
import org.scalatest._

class SinglebandGeoTiffReaderSpec extends FunSpec
    with RasterMatchers
    with GeoTiffTestUtils {

  def geoTiff(storage: String, cellType: String): SinglebandGeoTiff =
    SinglebandGeoTiff.compressed(geoTiffPath(s"uncompressed/$storage/${cellType}.tif"))

  def geoTiff(compression: String, storage: String, cellType: String): SinglebandGeoTiff =
    SinglebandGeoTiff.compressed(geoTiffPath(s"$compression/$storage/${cellType}.tif"))

  def expectedTile(t: CellType, f: (Int, Int) => Double): Tile = {
    val cols = 500
    val rows = 600

    val expected = geotrellis.raster.ArrayTile.empty(t, cols, rows)

    cfor(0)(_ < cols, _ + 1) { col =>
      cfor(0)(_ < rows, _ + 1) { row =>
        val v = f(col, row)
        expected.setDouble(col, row, v)
      }
    }

    expected
  }

  def writeExpectedTile(t: Tile, n: String): Unit = 
    geotrellis.raster.io.geotiff.writer.GeoTiffWriter.write(
      GeoTiff(
        t,
        geotrellis.vector.Extent(0.0, -10.0, 50.0, -4.0),
        geotrellis.proj4.LatLng
      ),
      geoTiffPath(s"$n.tif")
    )

  val compression = List("uncompressed", "lzw", "deflate", "packbits")
  val storage = List("striped", "tiled")
  val cellTypes = List("bit", "byte", "uint16", "int16", "int32", "float32", "float64")

  describe("Reading a single band geotiff") {
    it("must read Striped Bit aspect and match tiled byte converted to bitfile") {
      val actual = SinglebandGeoTiff.compressed(geoTiffPath("1band/aspect_bit_uncompressed_striped.tif")).tile
      val expected =
        SinglebandGeoTiff.compressed(geoTiffPath("1band/aspect_byte_uncompressed_tiled.tif"))
          .tile
          .toArrayTile
          .map { b => if(b == 0) 0 else 1 }
          .convert(BitCellType)
          .toArrayTile

      assertEqual(actual, expected)
    }

    it("must read Striped Bit aspect, convert to byte, and match gdal converted byte file") {
      val actual = GeoTiffReader.readSingleband(geoTiffPath("1band/aspect_bit_uncompressed_striped.tif")).tile.toArrayTile.convert(UByteCellType)
      val expected = GeoTiffReader.readSingleband(geoTiffPath("1band/aspect_bit-to-byte_uncompressed_striped.tif")).tile

      assertEqual(actual, expected)
    }

    it("should be able to collect points from a large sparse compressed geotiff that couldn't fit all into memory") {
      import geotrellis.vector._
      import scala.collection.mutable

      val n = "large-sparse-compressed.tif"
      val path = geoTiffPath(s"$n")
      val raster = SinglebandGeoTiff.compressed(path).raster

      val points = mutable.ListBuffer[Point]()
      val re = raster.rasterExtent
      raster.tile.foreach { (col, row, year) =>
        if(isData(year) && year != 0) {
          val (x, y) = re.gridToMap(col, row)
          points += PointFeature[Int](Point(x, y), year)
        }
      }
    }

    it("should find min and max of a large sparse raster, lzw compressed") {
      val n = "wm_depth.tif"
      val path = geoTiffPath(s"$n")
      val tile = SinglebandGeoTiff.compressed(path).tile

      val (min, max) = tile.findMinMaxDouble

      // Expected values from gdalinfo -stats
      min should be (-0.014291191473603 +- 0.000000000000001)
      max should be (4.4320001602173 +- 0.0000000000001)
    }
  }

  describe("Reading NBITS=1 GeoTiffs") {
    def testTiffBitValue(col: Int, row: Int): Double = {
      (col * row) % 2
    }

    val expected = expectedTile(BitCellType, testTiffBitValue _)
    val t = "bit"

    it("should read each variation of compression and striped/tiled") {
      println(s"Testing $t:")
      for(
        c <- compression;
        s <- storage
      ) {
        println(s"     Testing $c $s:")
        withClue(s"Failed for Compression $c, storage $s") {
          val tile = SinglebandGeoTiff.compressed(geoTiffPath(s"$c/$s/$t.tif")).tile.toArrayTile

          assertEqual(tile, expected)
        }
      }
    }
  }

  /*

  // These tests don't have a single (or predictable on the basis of compression/storage) celltype
  // this won't work unless we change the tiles or duplicate these tests

  describe("Reading UByte GeoTiffs") {
    def testTiffByteValue(col: Int, row: Int): Double = {
      (col * 1000.0 + row) % 128
    }

    val expected = expectedTile(UByteConstantNoDataCellType, testTiffByteValue _)
    val expectedRaw = expectedTile(UByteUserDefinedNoDataCellType(2), testTiffByteValue _)
    val t = "byte"

    it("should read each variation of compression and striped/tiled") {
      println(s"Testing $t:")
      for(
        c <- compression;
        s <- storage
      ) {
        println(s"     Testing $c $s:")
        withClue(s"Failed for Compression $c, storage $s") {
          val tile = SinglebandGeoTiff.compressed(geoTiffPath(s"$c/$s/$t.tif")).tile

          if(s == "striped") assertEqual(tile, expectedRaw) else assertEqual(tile, expected)
        }
      }
    }
  }
  */

  describe("Reading UInt16 GeoTiffs") {
    def testTiffShortValue(col: Int, row: Int): Double = {
      col + row
    }

    val expected = expectedTile(UShortCellType, testTiffShortValue _)
    val t = "uint16"

//    writeExpectedTile(expected, t)

    it("should read each variation of compression and striped/tiled") {
      println(s"Testing $t:")
      for(
        c <- compression;
        s <- storage
      ) {
          println(s"     Testing $c $s:")
          withClue(s"Failed for Compression $c, storage $s") {
            val tile = SinglebandGeoTiff.compressed(geoTiffPath(s"$c/$s/$t.tif")).tile.toArrayTile

            assertEqual(tile, expected)
          }
        }
    }
  }

  describe("Reading Int16 GeoTiffs") {
    def testTiffShortValue(col: Int, row: Int): Double = {
      math.pow(-1, (col + row) % 2) * (col + row)
    }

    val expected = expectedTile(ShortConstantNoDataCellType, testTiffShortValue _)
    val t = "int16"

//    writeExpectedTile(expected, t)

    it("should read each varition of compression and striped/tiled") {
      println(s"Testing $t:")
      for(
        c <- compression;
        s <- storage
      ) {
        println(s"     Testing $c $s:")
        withClue(s"Failed for Compression $c, storage $s") {
          val tile = SinglebandGeoTiff.compressed(geoTiffPath(s"$c/$s/$t.tif")).tile.toArrayTile

          assertEqual(tile, expected)
        }
      }
    }
  }

  describe("Reading UInt32 GeoTiffs") {
    def testTiffIntValue(col: Int, row: Int): Double = {
      col * 1000.0 + row
    }

    val expected = expectedTile(FloatCellType, testTiffIntValue _)
    val t = "uint32"

//    writeExpectedTile(expected, t)

    it("should read each varition of compression and striped/tiled") {
      println(s"Testing $t:")
      for(
        c <- compression;
        s <- storage
      ) {
        println(s"     Testing $c $s:")
        withClue(s"Failed for Compression $c, storage $s") {
          val tile = SinglebandGeoTiff.compressed(geoTiffPath(s"$c/$s/$t.tif")).tile.toArrayTile

          assertEqual(tile, expected)
        }
      }
    }
  }

  describe("Reading Int32 GeoTiffs") {
    def testTiffIntValue(col: Int, row: Int): Double = {
      math.pow(-1, (col + row) % 2) * (col * 1000.0 + row)
    }

    val expected = expectedTile(IntConstantNoDataCellType, testTiffIntValue _)
    val t = "int32"

//    writeExpectedTile(expected, t)

    it("should read each varition of compression and striped/tiled") {
      println(s"Testing $t:")
      for(
        c <- compression;
        s <- storage
      ) {
        println(s"     Testing $c $s:")
        withClue(s"Failed for Compression $c, storage $s") {
          val tile = SinglebandGeoTiff.compressed(geoTiffPath(s"$c/$s/$t.tif")).tile.toArrayTile

          assertEqual(tile, expected)
        }
      }
    }
  }

  describe("Reading Float32 GeoTiffs") {
    def testTiffFloatValue(col: Int, row: Int): Double = {
      math.pow(-1, (col + row) % 2) * (col * 1000.0 + row + (col / 1000.0))
    }

    val expected = expectedTile(FloatConstantNoDataCellType, testTiffFloatValue _)
    val t = "float32"

//    writeExpectedTile(expected, t)

    it("should read each varition of compression and striped/tiled") {
      println(s"Testing $t:")
      for(
        c <- compression;
        s <- storage
      ) {
        println(s"     Testing $c $s:")
        withClue(s"Failed for Compression $c, storage $s") {
          val tile = SinglebandGeoTiff.compressed(geoTiffPath(s"$c/$s/$t.tif")).tile.toArrayTile

          assertEqual(tile, expected)
        }
      }
    }
  }

  describe("Reading Float64 GeoTiffs") {
    def testTiffDoubleValue(col: Int, row: Int): Double = {
      math.pow(-1, (col + row) % 2) * (col * 1000.0 + row + (col / 1000.0))
    }

    val expected = expectedTile(DoubleUserDefinedNoDataCellType(-1.7976931348623157E308), testTiffDoubleValue _)
    val t = "float64"

    it("should read each varition of compression and striped/tiled") {
      println(s"Testing $t:")
      for(
        c <- compression;
        s <- storage
      ) {
        println(s"     Testing $c $s:")
        withClue(s"Failed for Compression $c, storage $s") {
          val tile = SinglebandGeoTiff.compressed(geoTiffPath(s"$c/$s/$t.tif")).tile.toArrayTile

          assertEqual(tile, expected)
        }
      }
    }
  }

  describe("Uncompressed SinglebandGeoTiffs over all storage and cell types") {
    it("should match the ArrayTile it builds from itself") {
      println(s"Testing toArrayTile:")
      for(
        s <- storage;
        t <- cellTypes
      ) {
        println(s"     Testing $s $t:")
        withClue(s"Failed for Storage $s, type $t") {
          val gtiff = geoTiff(s, t)
          val tile = gtiff.tile
          println("tiles", gtiff, gtiff.cellType, tile, tile.cellType, tile.toArrayTile, tile.toArrayTile.cellType)
          println(tile.get(0, 0), tile.toArrayTile.get(0, 0))
          assertEqual(tile, tile.toArrayTile)
        }
      }
    }

    it("should map") {
      println(s"Testing map:")
      for(
        s <- storage;
        t <- cellTypes
      ) {
        println(s"     Testing $s $t:")
        withClue(s"Failed for Storage $s, type $t") {
          val tile = geoTiff(s, t).tile
          val m1 = tile.map { z => z + 1 }
          val m2 = tile.toArrayTile.map { z => z + 1 }
          assertEqual(m1, m2)
        }
      }
    }

    it("should mapDouble") {
      println(s"Testing mapDouble:")
      for(
        s <- storage;
        t <- cellTypes
      ) {
        println(s"     Testing $s $t:")
        withClue(s"Failed for Storage $s, type $t") {
          val tile = geoTiff(s, t).tile
          val m1 = tile.mapDouble { z => z + 1.0 }
          val m2 = tile.toArrayTile.mapDouble { z => z + 1.0 }
          assertEqual(m1, m2)
        }
      }
    }
  }
}
