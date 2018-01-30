/*
 * Copyright 2016 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.raster.io.geotiff

import geotrellis.util.ByteReader
import geotrellis.raster._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.vector.Extent
import geotrellis.proj4.CRS
import geotrellis.raster.crop.Crop
import geotrellis.raster.resample.ResampleMethod

import spire.syntax.cfor._

case class SinglebandGeoTiff(
  tile: Tile,
  extent: Extent,
  crs: CRS,
  tags: Tags,
  options: GeoTiffOptions,
  overviews: List[GeoTiff[Tile]] = Nil
) extends GeoTiff[Tile] {
  val cellType = tile.cellType

  def mapTile(f: Tile => Tile): SinglebandGeoTiff =
    SinglebandGeoTiff(f(tile), extent, crs, tags, options, overviews)

  def withStorageMethod(storageMethod: StorageMethod): SinglebandGeoTiff =
    SinglebandGeoTiff(tile, extent, crs, tags, options.copy(storageMethod = storageMethod), overviews)

  def imageData: GeoTiffImageData =
    tile match {
      case gtt: GeoTiffTile => gtt
      case _ => tile.toGeoTiffTile(options)
    }

  def crop(subExtent: Extent, options: Crop.Options): SinglebandGeoTiff = {
    val raster: Raster[Tile] =
      this.raster.crop(subExtent, options)

    SinglebandGeoTiff(raster, subExtent, this.crs, this.tags, this.options, this.overviews)
  }

  def crop(colMax: Int, rowMax: Int): SinglebandGeoTiff =
    crop(0, 0, colMax, rowMax)

  def crop(colMin: Int, rowMin: Int, colMax: Int, rowMax: Int): SinglebandGeoTiff = {
    val raster: Raster[Tile] =
      this.raster.crop(colMin, rowMin, colMax, rowMax)

    SinglebandGeoTiff(raster, raster._2, this.crs, this.tags, this.options, this.overviews)
  }

  def crop(subExtent: Extent): SinglebandGeoTiff = crop(subExtent, Crop.Options.DEFAULT)

  def crop(subExtent: Extent, cellSize: CellSize, resampleMethod: ResampleMethod, strategy: OverviewStrategy): SinglebandRaster =
    getClosestOverview(cellSize, strategy)
      .crop(subExtent, Crop.Options(clamp = false))
      .resample(RasterExtent(subExtent, cellSize), resampleMethod, strategy)

  def resample(rasterExtent: RasterExtent, resampleMethod: ResampleMethod, strategy: OverviewStrategy): SinglebandRaster =
    getClosestOverview(cellSize, strategy)
      .raster
      .resample(rasterExtent, resampleMethod)

  def buildOverview(decimationFactor: Int, blockSize: Int, resampleMethod: ResampleMethod): SinglebandGeoTiff = {
    val segmentLayout: GeoTiffSegmentLayout = GeoTiffSegmentLayout(
      totalCols = tile.cols / decimationFactor,
      totalRows = tile.rows / decimationFactor,
      storageMethod = Tiled(blockSize, blockSize),
      interleaveMethod = PixelInterleave,
      bandType = BandType.forCellType(tile.cellType))

    @inline def padToBlockSize(x: Int, w: Int): Int = math.ceil(x / w.toDouble).toInt * w

    val overviewSegments = rasterExtent
      .rasterExtentFor(
        GridBounds(
          colMin = 0,
          rowMin = 0,
          colMax = padToBlockSize(tile.cols, blockSize * decimationFactor) - 1,
          rowMax = padToBlockSize(tile.rows, blockSize * decimationFactor) - 1))
      .withDimensions(
        segmentLayout.tileLayout.layoutCols,
        segmentLayout.tileLayout.layoutRows)

    val segments = for {
      layoutCol <- Iterator.range(0, segmentLayout.tileLayout.layoutCols)
      layoutRow <- Iterator.range(0, segmentLayout.tileLayout.layoutRows)
    } yield {
      val segmentExtent = overviewSegments.extentFor(GridBounds(layoutCol, layoutRow, layoutCol, layoutRow))
      val segmentTile = raster.resample(RasterExtent(segmentExtent, blockSize, blockSize), resampleMethod).tile

      ((layoutCol, layoutRow), segmentTile)
    }

    val storageMethod = Tiled(blockSize, blockSize)
    val overviewTile = GeoTiffBuilder[Tile].makeTile(
      segments, segmentLayout, cellType, options.compression
    )

    val overviewOptions = options.copy(
      subfileType = Some(ReducedImage),
      storageMethod = storageMethod)

    SinglebandGeoTiff(overviewTile, extent, crs, Tags.empty, overviewOptions)
  }
}

object SinglebandGeoTiff {

  def apply(
    tile: Tile,
    extent: Extent,
    crs: CRS
  ): SinglebandGeoTiff =
    SinglebandGeoTiff(tile, extent, crs, Tags.empty, GeoTiffOptions.DEFAULT)

  /** Read a single-band GeoTIFF file from a byte array.
    * The GeoTIFF will be fully decompressed and held in memory.
    */
  def apply(bytes: Array[Byte]): SinglebandGeoTiff =
    GeoTiffReader.readSingleband(bytes)

  /** Read a single-band GeoTIFF file from a byte array.
    * If decompress = true, the GeoTIFF will be fully decompressed and held in memory.
    */
  def apply(bytes: Array[Byte], decompress: Boolean, streaming: Boolean): SinglebandGeoTiff =
    GeoTiffReader.readSingleband(bytes, decompress, streaming)

  /** Read a single-band GeoTIFF file from the file at the given path.
    * The GeoTIFF will be fully decompressed and held in memory.
    */
  def apply(path: String): SinglebandGeoTiff =
    GeoTiffReader.readSingleband(path)

  def apply(path: String, e: Extent): SinglebandGeoTiff =
    GeoTiffReader.readSingleband(path, e)

  def apply(path: String, e: Option[Extent]): SinglebandGeoTiff =
    GeoTiffReader.readSingleband(path, e)

  /** Read a single-band GeoTIFF file from the file at the given path.
    * If decompress = true, the GeoTIFF will be fully decompressed and held in memory.
    */
  def apply(path: String, decompress: Boolean, streaming: Boolean): SinglebandGeoTiff =
    GeoTiffReader.readSingleband(path, decompress, streaming)

  def apply(byteReader: ByteReader): SinglebandGeoTiff =
    GeoTiffReader.readSingleband(byteReader)

  def apply(byteReader: ByteReader, e: Extent): SinglebandGeoTiff =
    GeoTiffReader.readSingleband(byteReader, e)

  def apply(byteReader: ByteReader, e: Option[Extent]): SinglebandGeoTiff =
    GeoTiffReader.readSingleband(byteReader, e)

  /** Read a single-band GeoTIFF file from the file at the given path.
    * The tile data will remain tiled/striped and compressed in the TIFF format.
    */
  def compressed(path: String): SinglebandGeoTiff =
    GeoTiffReader.readSingleband(path, false, false)

  /** Read a single-band GeoTIFF file from a byte array.
    * The tile data will remain tiled/striped and compressed in the TIFF format.
    */
  def compressed(bytes: Array[Byte]): SinglebandGeoTiff =
    GeoTiffReader.readSingleband(bytes, false, false)

  def streaming(path: String): SinglebandGeoTiff =
    GeoTiffReader.readSingleband(path, false, true)

  def streaming(byteReader: ByteReader): SinglebandGeoTiff =
    GeoTiffReader.readSingleband(byteReader, false, true)
}
