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
import org.apache.spark.rdd.{RDD => RDD[T]}

/**
 * RDD-based RepresentationHandler
 *
 * @author ctn
 *
 */
class RepresentationHandler(container: DDFHelper) extends ARepresentationHandler(container) with IHandleRepresentations {

	// The various representations for our DDF
	private val mReps = new HashMap[String, Any]

	private def getKeyFor(elementType: Class[_]): String = this.getKeyFor(classOf[RDD[_]], elementType)

	/**
	 * Gets an existing RDD representation for our {@link DDF} matching the given
	 * elementType, if any.
	 *
	 * @param elementType the type of the RDD element
	 *
	 * @return null if no matching {@link DDF}
	 */
	def get[T](elementType: Class[T]): Object = this.get(classOf[RDD[T]], elementType)

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
	def add[T](data: RDD[T])(implicit m: Manifest[T]): Unit = this.add(data, classOf[RDD[T]], m.erasure)

	/**
	 * Removes a representation from the set of existing representations.
	 */
	def remove[T](elementType: Class[T]): Unit = this.remove(classOf[RDD[T]], elementType)

}