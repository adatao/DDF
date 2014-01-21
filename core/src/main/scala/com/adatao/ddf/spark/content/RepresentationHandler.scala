/**
 *
 */
package com.adatao.ddf.spark.content

import com.adatao.ddf.content.IHandleRepresentations
import java.lang.Class
import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import com.adatao.ddf.content.ARepresentationHandler
import scala.reflect.Manifest
import com.adatao.ddf.spark.DDFHelper
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._

/**
 * RDD-based RepresentationHandler
 *
 * @author ctn
 *
 */
class RepresentationHandler(container: DDFHelper) extends ARepresentationHandler(container) with IHandleRepresentations {

	/**
	 * Sets a new and unique representation for our {@link DDF}, clearing out any existing ones
	 */
	def set[T](data: RDD[T])(implicit m: Manifest[T]) = {
		this.reset
		this.add(data)
	}

	/**
	 * Adds a new and unique representation for our {@link DDF}, keeping any existing ones
	 */
	def add[T](data: RDD[T])(implicit m: Manifest[T]): Unit = this.add(data, m.erasure)

	override def cleanup = {
		// Make sure we unpersist all RDDs we own
		mReps.foreach {
			kv ⇒
				{
					if (kv._2 != null) {
						val rdd = kv._2.asInstanceOf[RDD[_]]
						if (rdd != null) rdd.unpersist(false)
					}
				}
		}
		super.cleanup()
	}
}