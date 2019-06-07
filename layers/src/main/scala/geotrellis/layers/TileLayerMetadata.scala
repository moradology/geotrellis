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

package geotrellis.layers

import geotrellis.proj4.CRS
import geotrellis.raster._
import geotrellis.tiling._
import geotrellis.util._
import geotrellis.vector.{Extent, ProjectedExtent}

import cats.{Functor, Semigroup}
import cats.implicits._


/**
 * @param cellType    value type of each cell
 * @param layout      definition of the tiled raster layout
 * @param extent      Extent covering the source data
 * @param crs         CRS of the raster projection
 */
case class TileLayerMetadata[K](
  cellType: CellType,
  layout: LayoutDefinition,
  extent: Extent,
  crs: CRS,
  bounds: Bounds[K]
) {
  /** Transformations between tiling scheme and map references */
  def mapTransform: MapKeyTransform = layout.mapTransform
  /** TileLayout of the layout */
  def tileLayout: TileLayout = layout.tileLayout
  /** Full extent of the layout */
  def layoutExtent: Extent = layout.extent
  /** GridBounds of data tiles in the layout */
  def tileBounds: TileBounds = mapTransform(extent)

  def updateBounds(newBounds: Bounds[K])(implicit c: Component[K, SpatialKey]): TileLayerMetadata[K] =
    newBounds match {
      case kb: KeyBounds[K] => {
        val SpatialKey(minCol, minRow) = kb.minKey.getComponent[SpatialKey]
        val SpatialKey(maxCol, maxRow) = kb.maxKey.getComponent[SpatialKey]
        val kbExtent = mapTransform(GridBounds(minCol, minRow, maxCol, maxRow))

        kbExtent.intersection(extent) match {
          case Some(e) =>
            copy(bounds = newBounds, extent = e)
          case None =>
            copy(bounds = newBounds, extent = Extent(extent.xmin, extent.ymin, extent.xmin, extent.ymin))
        }
      }
      case EmptyBounds =>
        copy(bounds = newBounds, extent = Extent(extent.xmin, extent.ymin, extent.xmin, extent.ymin))
    }
}

object TileLayerMetadata {
  implicit def toLayoutDefinition(md: TileLayerMetadata[_]): LayoutDefinition =
    md.layout

  implicit def extentComponent[K]: GetComponent[TileLayerMetadata[K], Extent] =
    GetComponent(_.extent)

  implicit def crsComponent[K]: GetComponent[TileLayerMetadata[K], CRS] =
    GetComponent(_.crs)

  implicit def layoutComponent[K: SpatialComponent]: Component[TileLayerMetadata[K], LayoutDefinition] =
    Component(_.layout, (md, l) => md.copy(layout = l))

  implicit def boundsComponent[K: SpatialComponent]: Component[TileLayerMetadata[K], Bounds[K]] =
    Component(_.bounds, (md, b) => md.updateBounds(b))

  implicit def tileMetadataSemigroup[K: Boundable]: Semigroup[TileLayerMetadata[K]] =
    new Semigroup[TileLayerMetadata[K]] {
      def combine(x: TileLayerMetadata[K], y: TileLayerMetadata[K]): TileLayerMetadata[K] = {
        val combinedExtent       = x.extent combine y.extent
        val combinedBounds       = x.bounds combine y.bounds
        val combinedLayoutExtent = x.layout.extent combine y.layout.extent
        val combinedTileLayout   = x.layout.tileLayout combine y.layout.tileLayout

        x.copy(
          extent = combinedExtent,
          bounds = combinedBounds,
          layout = x.layout.copy(
            extent     = combinedLayoutExtent,
            tileLayout = combinedTileLayout
          )
        )
      }
    }

  implicit val tileLayerMetadataFunctor: Functor[TileLayerMetadata] = new Functor[TileLayerMetadata] {
    def map[A, B](fa: TileLayerMetadata[A])(f: A => B): TileLayerMetadata[B] =
      fa.copy(bounds = fa.bounds.map(f))
  }
}
