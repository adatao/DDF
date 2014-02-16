/**
 *
 */
package com.adatao.spark.ddf.content

import com.adatao.ddf.content.IHandleViews
import com.adatao.ddf.content.AViewHandler
import com.adatao.ddf.DDF
import shark.memstore2.TablePartition
import org.apache.spark.rdd.RDD
import com.adatao.spark.ddf.SparkDDFManager

/**
 * RDD-based ViewHandler
 *
 * @author ctn
 *
 */
class SparkViewHandler(container: SparkDDFManager) extends AViewHandler(container) with IHandleViews {
	object ViewFormat extends Enumeration {
		type ViewFormat = Value
		val DEFAULT, ARRAY_OBJECT, ARRAY_DOUBLE, TABLE_PARTITION, LABELED_POINT, LABELED_POINTS = Value
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
      case ViewFormat.DEFAULT ⇒ ViewHandler.getDefault(columns, container)
      case ViewFormat.ARRAY_OBJECT ⇒ ViewHandler.getArrayObject(columns, container)
      case ViewFormat.ARRAY_DOUBLE ⇒ ViewHandler.getArrayDouble(columns, container)
      case ViewFormat.TABLE_PARTITION ⇒ ViewHandler.getTablePartition(columns, container)
      case ViewFormat.LABELED_POINT ⇒ ViewHandler.getLabeledPoint(columns, container)
      case ViewFormat.LABELED_POINTS ⇒ ViewHandler.getLabeledPoints(columns, container)
      case _ ⇒ {}
		}
		null
	}

	protected def getImpl(columns: Array[Int], format: String): DDF = {
		this.get(columns, ViewFormat.withName(format))
	}

  override def sql2ddf(sqlCommand: String): DDF = {

    //Implementation here
    null
  }

  override def sql2text(sqlCommand: String): Array[String] = {

    //Implementation here
    null
  }

  override def getRandomSample(numSamples: Int): DDF = {

    //Implementation here
    null
  }

  override def reset(): Unit = {

    //Implementation here
  }
}

object ViewHandler {
  def getDefault(cols: Array[Int], container: SparkDDFManager): DDF = {

    null
  }

  def getArrayObject(cols: Array[Int], container: SparkDDFManager): DDF = {

    null
  }

  def getArrayDouble(cols: Array[Int], container: SparkDDFManager): DDF = {

    null
  }

  def getTablePartition(cols: Array[Int], container: SparkDDFManager): DDF = {

    null
  }

  def getLabeledPoint(cols: Array[Int], container: SparkDDFManager): DDF = {

    null
  }

  def getLabeledPoints(cols: Array[Int], container: SparkDDFManager): DDF = {


   null
  }
}