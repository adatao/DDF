/**
 *
 */
package com.adatao.spark.ddf.content

import com.adatao.ddf.content.IHandleRepresentations
import com.adatao.ddf.content.Schema.ColumnType
import java.lang.Class
import scala.reflect.Manifest
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import shark.api.Row
import com.adatao.ddf.{ DDF, DDFManager }
import org.apache.spark.mllib.regression.LabeledPoint
import com.adatao.spark.ddf.content.RepresentationHandler._
import com.adatao.ddf.exception.DDFException

/**
 * RDD-based SparkRepresentationHandler
 *
 * @author ctn
 *
 */

class RepresentationHandler(mDDF: DDF) extends com.adatao.ddf.content.RepresentationHandler(mDDF) {
	protected def getDefaultRepresentationImpl(): RDD[Row] = {

		if (mDDF.getRepresentationHandler.get(classOf[Row]) == null) {
			throw new Exception("Please load theDDFManager representation")
		}
		val rdd = mDDF.getRepresentationHandler.get(classOf[Row]).asInstanceOf[RDD[Row]]
		rdd
	}

	override def getDefaultDataType: Class[_] = classOf[RDD[Row]];

	/**
	 * Creates a specific representation
	 */
	override def createRepresentation(rowType: Class[_]): Object = {
		val schema = mDDF.getSchemaHandler
		val numCols = schema.getNumColumns.toInt

		val extractors = schema.getColumns().map(colInfo ⇒ doubleExtractor(colInfo.getType)).toArray

		val rdd = getDefaultRepresentationImpl();
		rowType match {

			case ARRAY_OBJECT ⇒ getRDDArrayObject(rdd, numCols)

			case ARRAY_DOUBLE ⇒ getRDDArrayDouble(rdd, numCols, extractors)

			case LABELED_POINT ⇒ getRDDLabeledPoint(rdd, numCols, extractors)

			case _ ⇒ throw new DDFException("rowType not supported")
		}
	}

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

	private def forAllReps[T](f: RDD[_] ⇒ Any) {
		mReps.foreach {
			kv ⇒ if (kv._2 != null) f(kv._2.asInstanceOf[RDD[_]])
		}
	}

	override def cacheAll = {
		forAllReps({
			rdd: RDD[_] ⇒
				if (rdd != null) {
					mLog.info(this.getClass() + ": Persisting " + rdd)
					rdd.persist
				}
		})
	}

	override def uncacheAll = {
		forAllReps({
			rdd: RDD[_] ⇒
				if (rdd != null) {
					mLog.info(this.getClass() + ": Unpersisting " + rdd)
					rdd.unpersist(false)
				}
		})
	}
}

object RepresentationHandler {
	/**
	 * Supported Representations
	 */
	val ARRAY_DOUBLE = classOf[Array[Double]]
	val ARRAY_OBJECT = classOf[Array[Object]]
	val LABELED_POINT = classOf[LabeledPoint]

	/**
	 *
	 */
	def getRDDArrayObject(rdd: RDD[Row], numCols: Int): RDD[Array[Object]] = {
		rdd.map {
			row ⇒ rowToArrayObject(row, numCols)
		}
	}

	def getRDDArrayDouble(rdd: RDD[Row], numCols: Int, extractors: Array[Object ⇒ Double]): RDD[Array[Double]] = {
		rdd.map(row ⇒ rowToArrayDouble(row, numCols, extractors)).filter(row ⇒ row != null)
	}

	def getRDDLabeledPoint(rdd: RDD[Row], numCols: Int, extractors: Array[Object ⇒ Double]): RDD[LabeledPoint] = {
		rdd.map(row ⇒ rowToLabeledPoint(row, numCols, extractors)).filter(point ⇒ point != null)
	}

	def rowToArrayObject(row: Row, numCols: Int): Array[Object] = {
		val array = new Array[Object](numCols)
		var i = 0
		while (i < numCols) {
			array(i) = row.getPrimitive(i)
			i += 1
		}
		array
	}

	def rowToArrayDouble(row: Row, numCols: Int, extractors: Array[Object ⇒ Double]): Array[Double] = {
		val array = new Array[Double](numCols)
		var i = 0
		var isNull = false

		while (i < numCols && !isNull) {
			val obj = row.getPrimitive(i)
			if (obj == null) {
				isNull = true
			}
			else {
				array(i) = extractors(i)(obj)
			}
			i += 1
		}
		if (isNull) null else array
	}

	def rowToLabeledPoint(row: Row, numCols: Int, extractors: Array[Object ⇒ Double]): LabeledPoint = {
		val features = new Array[Double](numCols - 1)
		var label = 0.0
		var isNull = false
		var i = 0

		while (i < numCols && !isNull) {
			val obj = row.getPrimitive(i)
			if (obj == null) {
				isNull = true
			}
			else {
				if (i < numCols - 1) {
					features(i) = extractors(i)(obj)
				}
				else {
					label = extractors(i)(obj)
				}
			}
			i += 1
		}
		if (isNull) null else new LabeledPoint(label, features)
	}

	def doubleExtractor(colType: ColumnType): Object ⇒ Double = colType match {
		case ColumnType.DOUBLE ⇒ {
			case obj ⇒ obj.asInstanceOf[Double]
		}

		case ColumnType.INT ⇒ {
			case obj ⇒ obj.asInstanceOf[Int].toDouble
		}

		case ColumnType.STRING ⇒ {
			case _ ⇒ throw new Exception("Cannot convert string to double.")
		}
	}
}
