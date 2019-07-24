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

package geotrellis.spark.reproject

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.reproject._
import geotrellis.raster.resample._
import geotrellis.spark.resample._
import geotrellis.spark._
import geotrellis.spark.ingest._
import geotrellis.vector._
import geotrellis.util._

import org.apache.spark.rdd._

import scala.reflect.ClassTag

object ProjectedExtentComponentReproject {


  /** Reproject the given RDD and modify the key with the new CRS and extent
    */
  def apply[K: Component[?, ProjectedExtent], V <: CellGrid[Int]: (? => TileReprojectMethods[V])](
    rdd: RDD[(K, V)],
    destCrs: CRS,
    options: RasterReprojectOptions
  ): RDD[(K, V)] =
    rdd.map { case (key, tile) =>
      val ProjectedExtent(extent, crs) = key.getComponent[ProjectedExtent]
      val Raster(newTile , newExtent) = {
        // TODO revisit this before merge to make sure things work as expected
        val resampleGrid =
          if (options.targetRasterExtent.isDefined) {
            TargetRegion[Long](options.targetRasterExtent.get.toGridType[Long])
          } else if (options.targetCellSize.isDefined) {
            TargetCellSize[Long](options.targetCellSize.get)
          } else if (options.parentGridExtent.isDefined) {
            TargetGrid[Long](options.parentGridExtent.get)
          } else IdentityResampleGrid
        tile.reproject(extent, crs, destCrs, resampleGrid)
      }
      val newKey = key.setComponent(ProjectedExtent(newExtent, destCrs))
      (newKey, newTile)
    }
}
