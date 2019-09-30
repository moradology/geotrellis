/*
 * Copyright 2018 Azavea
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

package geotrellis.spark.store.hadoop.geotiff

import geotrellis.raster.io.geotiff._
import geotrellis.raster.{CellGrid, Raster, RasterExtent}
import geotrellis.raster.resample.{RasterResampleMethods, ResampleMethod, TargetGridExtent, TargetCellSize}
import geotrellis.store.LayerId
import geotrellis.layer.{SpatialKey, ZoomedLayoutScheme}
import geotrellis.vector.{Extent, ProjectedExtent}
import geotrellis.raster.crop.Crop
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.reproject.RasterReprojectMethods
import geotrellis.raster.merge.RasterMergeMethods
import geotrellis.spark.reproject.RasterReprojectOptions
import geotrellis.util.ByteReader
import geotrellis.util.annotations.experimental
import geotrellis.store.LayerId

import cats.effect.IO
import cats.syntax.apply._
import cats.syntax.either._

import java.net.URI

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

/**
  * @define experimental <span class="badge badge-red" style="float: right;">EXPERIMENTAL</span>@experimental
  */
@experimental trait GeoTiffLayerReader[M[T] <: Traversable[T]] {
  implicit def getByteReader(uri: URI): ByteReader

  val attributeStore: AttributeStore[M, GeoTiffMetadata]
  val layoutScheme: ZoomedLayoutScheme
  val resampleMethod: ResampleMethod
  val strategy: OverviewStrategy

  implicit val ec: ExecutionContext
  implicit val cs      = IO.contextShift(ec)

  @experimental def read[V <: CellGrid[Int]: GeoTiffReader: ClassTag]
    (layerId: LayerId)
    (x: Int, y: Int)
    (implicit rep: Raster[V] => RasterReprojectMethods[Raster[V]],
              res: Raster[V] => RasterResampleMethods[Raster[V]],
                m: Raster[V] => RasterMergeMethods[V]): Raster[V] = {
    val layout =
      layoutScheme
        .levelForZoom(layerId.zoom)
        .layout

    val mapTransform = layout.mapTransform
    val keyExtent: Extent = mapTransform(SpatialKey(x, y))

    val index: fs2.Stream[IO, GeoTiffMetadata] =
      fs2.Stream.fromIterator[IO, GeoTiffMetadata](attributeStore.query(layerId.name, ProjectedExtent(keyExtent, layoutScheme.crs)).toIterator)

    val readRecord: GeoTiffMetadata => fs2.Stream[IO, Option[Raster[V]]] = { md =>
      fs2.Stream eval IO.shift(ec) *> IO {
        val tiff = GeoTiffReader[V].read(md.uri, streaming = true)
        val reprojectedKeyExtent = keyExtent.reproject(layoutScheme.crs, tiff.crs)

        // crop is unsafe, let's double check that we have a correct extent
        tiff
          .extent
          .intersection(reprojectedKeyExtent)
          .map { ext =>
            tiff
              .getClosestOverview(layout.cellSize, strategy)
              .crop(ext, Crop.Options(clamp = false))
              .raster
              .reproject(tiff.crs, layoutScheme.crs, Some(TargetCellSize(layout.cellSize)))
              .resample(TargetGridExtent(RasterExtent(keyExtent, layoutScheme.tileSize, layoutScheme.tileSize)))
          }
      }
    }

    (index flatMap readRecord)
      .compile
      .toVector.map(_.flatten.reduce(_ merge _))
      .attempt
      .unsafeRunSync()
      .valueOr(throw _)
  }

  @experimental def readAll[V <: CellGrid[Int]: GeoTiffReader: ClassTag]
    (layerId: LayerId)
    (implicit rep: Raster[V] => RasterReprojectMethods[Raster[V]],
              res: Raster[V] => RasterResampleMethods[Raster[V]]): Traversable[Raster[V]] = {
    val layout =
      layoutScheme
        .levelForZoom(layerId.zoom)
        .layout

    val index: fs2.Stream[IO, GeoTiffMetadata] =
      fs2.Stream.fromIterator[IO, GeoTiffMetadata](attributeStore.query(layerId.name).toIterator)

    val readRecord: GeoTiffMetadata => fs2.Stream[IO, Raster[V]] = { md =>
      fs2.Stream eval IO.shift(ec) *> IO {
        val tiff = GeoTiffReader[V].read(md.uri, streaming = true)
        tiff
          .crop(tiff.extent, layout.cellSize)
          .reproject(tiff.crs, layoutScheme.crs)
          .resample(TargetGridExtent(RasterExtent(tiff.extent, layoutScheme.tileSize, layoutScheme.tileSize)))
      }
    }

    index
      .map(readRecord)
      .parJoinUnbounded
      .compile
      .toVector
      .attempt
      .unsafeRunSync()
      .valueOr(throw _)
  }
}
