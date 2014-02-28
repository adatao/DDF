/**
 *
 */
package com.adatao.spark.ddf.content

import com.adatao.ddf.content.IHandleViews
import com.adatao.ddf.DDF
import com.adatao.spark.ddf.SparkDDFManager

/**
 * RDD-based ViewHandler
 *
 * @author ctn
 *
 */
class ViewHandler(mDDF: DDF) extends com.adatao.ddf.content.ViewHandler(mDDF) with IHandleViews {

	object ViewFormat extends Enumeration {
		type ViewFormat = Value
		val DEFAULT, ARRAY_OBJECT, ARRAY_DOUBLE, TABLE_PARTITION, LABELED_POINT, LABELED_POINTS = Value
	}

	import ViewFormat._

	/**
	 * Same as {@link #get(int[], int)}, but accepts a scala.Enumeration for format instead.
	 *
	 * @param columns
	 * @param format
	 * A scala.Enumeration that will be converted to an integer by calling
	 * formatEnum.toString()
	 * @return
	 */
	def get(columns: Array[Int], format: ViewFormat): DDF = {
		format match {
			case ViewFormat.DEFAULT ⇒ ViewHandler.getDefault(columns, mDDF)
			case ViewFormat.ARRAY_OBJECT ⇒ ViewHandler.getArrayObject(columns, mDDF)
			case ViewFormat.ARRAY_DOUBLE ⇒ ViewHandler.getArrayDouble(columns, mDDF)
			case ViewFormat.TABLE_PARTITION ⇒ ViewHandler.getTablePartition(columns, mDDF)
			case ViewFormat.LABELED_POINT ⇒ ViewHandler.getLabeledPoint(columns, mDDF)
			case ViewFormat.LABELED_POINTS ⇒ ViewHandler.getLabeledPoints(columns, mDDF)
			case _ ⇒ {}
		}
		null
	}

	protected def getImpl(columns: Array[Int], format: String): DDF = {
		this.get(columns, ViewFormat.withName(format))
	}

	override def getRandomSample(numSamples: Int): DDF = {

		//Implementation here
		null
	}
}

object ViewHandler {
	def getDefault(cols: Array[Int], theDDF: DDF): DDF = {

		null
	}

	def getArrayObject(cols: Array[Int], theDDF: DDF): DDF = {

		null
	}

	def getArrayDouble(cols: Array[Int], theDDF: DDF): DDF = {

		null
	}

	def getTablePartition(cols: Array[Int], theDDF: DDF): DDF = {

		null
	}

	def getLabeledPoint(cols: Array[Int], theDDF: DDF): DDF = {

		null
	}

	def getLabeledPoints(cols: Array[Int], theDDF: DDF): DDF = {

		null
	}
}