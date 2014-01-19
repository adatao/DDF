/**
 *
 */
package com.adatao.DDF.spark

import com.adatao.DDF.IHandleRepresentations
import java.lang.Class
import scala.collection.mutable.HashMap
import com.adatao.DDF.ADataFrameFunctionalGroupHandler
import org.apache.spark.rdd.RDD
import com.adatao.DDF.ARepresentationHandler

/**
 * @author ctn
 *
 */
class RepresentationHandler(container: DataFrameImplementor) extends ARepresentationHandler(container) with IHandleRepresentations {

	// The various representations for our DataFrame
	private val mReps = new HashMap[String, Any]

	private def getKeyFor(elementType: Class[_]): String = this.getKeyFor(classOf[RDD[_]], elementType)

	/**
	 * Gets an existing RDD representation for our {@link DataFrame} matching the given
	 * elementType, if any.
	 *
	 * @param elementType the type of the RDD element
	 *
	 * @return null if no matching {@link DataFrame}
	 */
	def get(elementType: Class[_]): Object = this.get(classOf[RDD[_]], elementType)

	/**
	 * Sets a new and unique representation for our {@link DataFrame}, clearing out any existing ones
	 */
	def set(data: Object, elementType: Class[_]) = {
		this.reset
		this.add(data, classOf[RDD[_]], elementType)
	}

	/**
	 * Adds a new and unique representation for our {@link DataFrame}, keeping any existing ones
	 */
	def add(data: Object, elementType: Class[_]): Unit = this.add(data, classOf[RDD[_]], elementType)

}