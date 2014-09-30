package com.adatao.spark.ddf.analytics

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.api.java.JavaRDD
import com.adatao.spark.ddf.analytics.util.Vector
import com.adatao.spark.ddf.analytics.util.Vectors
import com.adatao.spark.ddf.analytics.util.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import com.adatao.spark.ddf.analytics.util.Vector
import com.adatao.spark.ddf.analytics.util.Vectors

class HashingTF(val numFeatures: Int) extends Serializable {

  def this() = this(1 << 20)

  /**
   * Returns the index of the input term.
   */
  def indexOf(term: Any): Int = nonNegativeMod(term.##, numFeatures)

  /**
   * Transforms the input document into a sparse term frequency vector.
   */
  def transform(document: Iterable[_]): Vector = {
    val termFrequencies = mutable.HashMap.empty[Int, Double]
    document.foreach { term =>
      val i = indexOf(term)
      termFrequencies.put(i, termFrequencies.getOrElse(i, 0.0) + 1.0)
    }
    Vectors.sparse(numFeatures, termFrequencies.toSeq)
  }


  /**
   * Transforms the input document to term frequency vectors.
   */
  def transform[D <: Iterable[_]](dataset: RDD[D]): RDD[Vector] = {
    dataset.map(this.transform)
  }

  /* Calculates 'x' modulo 'mod', takes to consideration sign of x,
  * i.e. if 'x' is negative, than 'x' % 'mod' is negative too
  * so function return (x % mod) + mod in that case.
  */
  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }
}
