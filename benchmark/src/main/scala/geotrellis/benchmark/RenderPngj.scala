/*
 * Copyright (c) 2014 Azavea.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.benchmark

import geotrellis.engine._
import geotrellis.engine.op.local._
import geotrellis.engine.op.global._
import geotrellis.engine.render._
import geotrellis.engine.io._
import geotrellis.raster._
import geotrellis.raster.op.global._
import geotrellis.raster.op.local._
import geotrellis.raster.op.stats._
import geotrellis.raster.render._
import geotrellis.raster.render.png._

import com.google.caliper.Param
import ar.com.hjg.pngj._
import java.io.ByteArrayOutputStream
import spire.syntax.cfor._

object PngjRendererBenchmark extends BenchmarkRunner(classOf[PngjRendererBenchmark])
class PngjRendererBenchmark extends OperationBenchmark {
  val name = "SBN_farm_mkt"
  val names = Array("SBN_farm_mkt", "SBN_RR_stops_walk", "SBN_inc_percap", "SBN_street_den_1k")
  val weights = Array(2, 1, 5, 2)
  val colors = Array(0x0000FF, 0x0080FF, 0x00FF80, 0xFFFF00, 0xFF8000, 0xFF0000)


  @Param(Array("512", "1024", "2048", "4096"))
  var size: Int = 0

  var renderer: Renderer = null
  var raster: Tile = null
  var rendered: Tile = null
  var data: Array[Int] = null

  override def setUp() {
    val re = getRasterExtent(names(0), size, size)
    val total = weights.sum
    val rs = (0 until names.length).map(i => Multiply(RasterSource(names(i), re).get, weights(i)))
    val weightedAdd = Add(rs)
    val divided = Divide(weightedAdd, total)
    val r = divided.rescale(1, 100)
    val histogram = get(r.histogram)
    val colorBreaks = get(ColorBreaks(histogram, colors))
    val breaks = colorBreaks.limits
    val cs = colorBreaks.colors

    renderer = Renderer(breaks, cs, 0, histogram)
    raster = r
    data = r.asInstanceOf[ArrayTile].toArray
    rendered = renderer.render(raster)
  }

  def timeRenderer(reps: Int) = run(reps)(runRenderer)
  def runRenderer = renderer.render(raster)

  def timeEncoder(reps: Int) = run(reps)(runEncoder)
  def runEncoder = {
    val r2 = renderer.render(raster)
    justRunTheThing(r2)
  }

  def timeJustEncoder(reps: Int) = run(reps)(runJustEncoder)
  def runJustEncoder = justRunTheThing(rendered)

  def justRunTheThing(tile: Tile) = {
    val iinfo = new ImageInfo(tile.cols, tile.rows, 16, false)
    val baos = new ByteArrayOutputStream
    val writer = new PngWriter(baos, iinfo)
    val iLine = new ImageLineInt(iinfo)
    cfor(0)(_ < tile.cols, _ + 1) { x =>
      cfor(0)(_ < tile.rows, _ + 1) { y =>
        val c = tile.get(x, y)
        ImageLineHelper.setPixelRGB8(iLine, x, c & 0xff, c << 8 & 0xff, c << 16 & 0xff)
      }
    }
    cfor(0)(_ < tile.rows, _ + 1) { row =>
      writer.writeRow(iLine)
    }
    writer.end
    baos.flush()
    val arr = baos.toByteArray
    baos.close()
  }
}
