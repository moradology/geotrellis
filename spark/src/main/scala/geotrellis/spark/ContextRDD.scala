package geotrellis.spark

import geotrellis.raster._
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd._
import spray.json.JsonFormat

import scala.reflect.ClassTag

object ContextRDD {
  def apply[K, V, M](rdd: RDD[(K, V)], metadata: M): RDD[(K, V)] with Metadata[M] =
    new ContextRDD(rdd, metadata)

  implicit def tupleToContextRDD[K, V, M](tup: (RDD[(K, V)], M)): ContextRDD[K, V, M] =
    new ContextRDD(tup._1, tup._2)
}

class ContextRDD[K, V, M](val rdd: RDD[(K, V)], val metadata: M) extends RDD[(K, V)](rdd) with Metadata[M] {
  override val partitioner = rdd.partitioner

  def compute(split: Partition, context: TaskContext) =
    rdd.iterator(split, context)

  def getPartitions: Array[Partition] =
    rdd.partitions
}
