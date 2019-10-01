/*
 * Copyright 2017 Azavea
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

package geotrellis.raster.reproject

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.util.MethodExtensions

import spire.math.Integral

abstract class RasterTileFeatureReprojectMethods[
  T <: CellGrid[Int]: (? => TileReprojectMethods[T]),
  D
](val self: TileFeature[Raster[T], D]) extends MethodExtensions[TileFeature[Raster[T], D]] {

  def reproject(transform: Transform, inverseTransform: Transform, resampleTarget: ResampleTarget): TileFeature[Raster[T], D] =
    TileFeature(self.tile.tile.reproject(self.tile.extent, transform, inverseTransform, resampleTarget), self.data)

  def reproject(src: CRS, dest: CRS, resampleTarget: ResampleTarget): TileFeature[Raster[T], D] =
    TileFeature(self.tile.tile.reproject(self.tile.extent, src, dest, resampleTarget), self.data)

}
