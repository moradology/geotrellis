package geotrellis.spark.summary.polygonal

import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.testfiles._
import geotrellis.raster.summary.polygonal._

import geotrellis.vector._
import geotrellis.vector.summary.polygonal._

import org.scalatest._

class FeatureRDDSpec extends FunSpec
    with Matchers
    with TestEnvironment {

  describe("Zonal summary on an RDD of features") {
    it("should compute the area of features under a zone") {
      val lowerExtent = Extent(1, 1, 7, 3) // Partially intersects
      val middleExtent = Extent(3, 3, 5, 4) // Contained
      val upperExtent = Extent(1, 4, 7, 6) // Partially intersects

      val polygon = Polygon( (2, 2), (4, 6), (6, 2), (2, 2) )

      val featureRdd = sc.parallelize(Array(lowerExtent, middleExtent, upperExtent).map { e => Feature(e.toPolygon, e.area) })
      val result = featureRdd.polygonalSummary(polygon, 0.0)(
        PolygonalSummaryHandler({ feature: Feature[Polygon, Double] => feature.data })
                           ({ (polygon, feature) => polygon.intersection(feature.geom).asMultiPolygon.map(_.area).getOrElse(0.0) })
                           ({ (v1, v2) => v1 + v2 })
      )
      val expected = polygon.area - 0.5

      result should be (expected)
    }
  }
}
