/**
 *
 */
package com.adatao.spark.ddf.analytics

import org.apache.spark.rdd.RDD

/**
 * Provides implicit extensions for RDDs
 *
 * @author ctn
 *
 */
class RDDImplicits[T](rdd: RDD[T]) {
	/**
	 * Safely reduces the elements of this RDD using the specified commutative and associative binary operator.
	 * "Safety" here means that:
	 *
	 * - If all of the partitions are empty, the result is null
	 * - If at least one of the partitions is non-empty, the resuls is the reduce() of the non-empty partitions
	 *
	 * @param f - the commutative-associative function operating on T
	 * @param valueIfEmpty - optional value to be returned if collection is empty. Default = null.asInstanceOf[T]
	 * @return - a reduce() operation, operating across all partitions of T
	 */
	def safeReduce(f: (T, T) ⇒ T, valueIfEmpty: T = null.asInstanceOf[T]): T = {
		try {
			rdd.reduce(f)
		}
		catch {
			case usoe: UnsupportedOperationException ⇒ valueIfEmpty
		}
	}

	/**
	 * Safely returns the first element of this RDD.
	 *
	 * @param valueIfEmpty - option value to be returned if collection is empty
	 * @return the first element, or valueIfEmpty if the collection is empty
	 */
	def safeFirst(valueIfEmpty: T = null.asInstanceOf[T]): T = {
		try {
			rdd.first()
		}
		catch {
			case usoe: UnsupportedOperationException ⇒ valueIfEmpty
		}
	}

}

object RDDImplicits {
	implicit def rddImplicits[T](rdd: RDD[T]) = new RDDImplicits[T](rdd)
}
