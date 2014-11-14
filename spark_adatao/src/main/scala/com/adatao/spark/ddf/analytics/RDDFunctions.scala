package com.adatao.spark.ddf.analytics

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.spark.HashPartitioner
import org.apache.spark.util.{Utils => SUtils}
import org.apache.spark.SparkContext._

/**
 * author: daoduchuan
 */
class RDDFunctions[T: ClassTag](self: RDD[T]) {


  /**
   * Reduces the elements of this RDD in a multi-level tree pattern.
   *
   * @param depth suggested depth of the tree (default: 2)
   * @see [[org.apache.spark.rdd.RDD#reduce]]
   */
  def treeReduce(f: (T, T) => T, depth: Int = 2): T = {
    require(depth >= 1, s"Depth must be greater than or equal to 1 but got $depth.")
    val cleanF = self.context.clean(f)
    val reducePartition: Iterator[T] => Option[T] = iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(cleanF))
      } else {
        None
      }
    }
    val partiallyReduced = self.mapPartitions(it => Iterator(reducePartition(it)))
    val op: (Option[T], Option[T]) => Option[T] = (c, x) => {
      if (c.isDefined && x.isDefined) {
        Some(cleanF(c.get, x.get))
      } else if (c.isDefined) {
        c
      } else if (x.isDefined) {
        x
      } else {
        None
      }
    }
    RDDFunctions.fromRDD(partiallyReduced).treeAggregate(Option.empty[T])(op, op, depth)
      .getOrElse(throw new UnsupportedOperationException("empty collection"))
  }

  /**
   * Aggregates the elements of this RDD in a multi-level tree pattern.
   *
   * @param depth suggested depth of the tree (default: 2)
   * @see [[org.apache.spark.rdd.RDD#aggregate]]
   */
  def treeAggregate[U: ClassTag](zeroValue: U)(
    seqOp: (U, T) => U,
    combOp: (U, U) => U,
    depth: Int = 2): U = {
    require(depth >= 1, s"Depth must be greater than or equal to 1 but got $depth.")
    if (self.partitions.size == 0) {
      return SUtils.clone(zeroValue, self.context.env.closureSerializer.newInstance())
    }
    val cleanSeqOp = self.context.clean(seqOp)
    val cleanCombOp = self.context.clean(combOp)
    val aggregatePartition = (it: Iterator[T]) => it.aggregate(zeroValue)(cleanSeqOp, cleanCombOp)
    var partiallyAggregated = self.mapPartitions(it => Iterator(aggregatePartition(it)))
    var numPartitions = partiallyAggregated.partitions.size
    val scale = math.max(math.ceil(math.pow(numPartitions, 1.0 / depth)).toInt, 2)
    // If creating an extra level doesn't help reduce the wall-clock time, we stop tree aggregation.
    while (numPartitions > scale + numPartitions / scale) {
      numPartitions /= scale
      val curNumPartitions = numPartitions
      partiallyAggregated = partiallyAggregated.mapPartitionsWithIndex { (i, iter) =>
        iter.map((i % curNumPartitions, _))
      }.reduceByKey(new HashPartitioner(curNumPartitions), cleanCombOp).values

    }
    partiallyAggregated.reduce(cleanCombOp)
  }
}

object RDDFunctions {

  /** Implicit conversion from an RDD to RDDFunctions. */
  implicit def fromRDD[T: ClassTag](rdd: RDD[T]) = new RDDFunctions[T](rdd)
}
