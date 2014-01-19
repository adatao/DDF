/**
 *
 */
package com.adatao.ddf.spark

import com.adatao.ddf.IHandleRepresentations
import java.lang.Class
import scala.collection.mutable.HashMap
import com.adatao.ddf.ADDFFunctionalGroupHandler
import org.apache.spark.rdd.RDD
import com.adatao.ddf.ARepresentationHandler

/**
 * @author ctn
 *
 */
class RepresentationHandler(container: DDFImplementor) extends ARepresentationHandler(container) with IHandleRepresentations {

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
	def get(elementType: Class[_]): Object = this.get(classOf[RDD[_]], elementType)

	/**
	 * Sets a new and unique representation for our {@link DDF}, clearing out any existing ones
	 */
	def set(data: Object, elementType: Class[_]) = {
		this.reset
		this.add(data, classOf[RDD[_]], elementType)
	}

	/**
	 * Adds a new and unique representation for our {@link DDF}, keeping any existing ones
	 */
	def add(data: Object, elementType: Class[_]): Unit = this.add(data, classOf[RDD[_]], elementType)

}