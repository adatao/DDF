/**
 *
 */
package com.adatao.ddf.spark.content

import com.adatao.ddf.content.IHandleViews
import com.adatao.ddf.spark.DDFHelper
import com.adatao.ddf.content.AViewHandler
import com.adatao.ddf.DDF

/**
 * RDD-based ViewHandler
 *
 * @author ctn
 *
 */
class ViewHandler(container: DDFHelper) extends AViewHandler(container) with IHandleViews {
	object ViewFormat extends Enumeration {
		type ViewFormat = Value
		val DEFAULT, ARRAY_OBJECT, ARRAY_DOUBLE, TABLE_PARTITION = Value
	}
	import ViewFormat._

	/**
	 * Same as {@link #get(int[], int)}, but accepts a scala.Enumeration for format instead.
	 *
	 * @param columns
	 * @param formatEnum
	 *          A scala.Enumeration that will be converted to an integer by calling
	 *          formatEnum.toString()
	 * @return
	 */
	def get(columns: Array[Int], formatEnum: ViewFormat): DDF = {
		formatEnum match {
			case ViewFormat.DEFAULT ⇒ {}
			case ViewFormat.ARRAY_OBJECT ⇒ {}
			case ViewFormat.ARRAY_DOUBLE ⇒ {}
			case ViewFormat.TABLE_PARTITION ⇒ {}
			case _ ⇒ {}
		}
		null
	}

	protected def getImpl(columns: Array[Int], format: String): DDF = {
		this.get(columns, ViewFormat.withName(format))
	}
}