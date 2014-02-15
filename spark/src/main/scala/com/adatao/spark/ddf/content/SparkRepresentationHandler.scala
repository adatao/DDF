/**
 *
 */
package com.adatao.spark.ddf.content

import com.adatao.ddf.content.{IHandleRepresentations, ARepresentationHandler}
import com.adatao.ddf.content.Schema.ColumnType
import com.adatao.ddf.spark.etl.SparkPersistenceHandler
import java.lang.Class
import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import scala.reflect.Manifest
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import shark.memstore2.TablePartition
import shark.api.{Row, TableRDD}
import com.adatao.spark.ddf.SparkDDFManager

/**
 * RDD-based SparkRepresentationHandler
 *
 * @author ctn
 *
 */
class SparkRepresentationHandler(container: SparkDDFManager) extends ARepresentationHandler(container) with IHandleRepresentations {

  /**
   *
   */
  protected def getRepresentationImpl(elementType: Class[_]): Object = {

    val schema= container.getMetaDataHandler.getSchema

    val cols= schema.getColumns()
    val numCols= schema.getNumColumns
    val reprHandler= container.getRepresentationHandler

    if(reprHandler.get(classOf[Row]) == null){
      throw new Exception("Please load container representation with sqlLoad")
    }

    //val cmd = "select " + columnNames.mkString(", ") + " from " + schema.getTableName()

    val rdd= container.getRepresentationHandler.get(classOf[Row]).asInstanceOf[RDD[Row]]
    //val tableRDD: TableRDD= container.getPersistenceHandler.asInstanceOf[SparkPersistenceHandler].sql2rdd(cmd)

    elementType match {

      case arrayObject if arrayObject == classOf[Array[Object]] => SparkRepresentationHandler.getRDDArrayObject(rdd, numCols)

      case arrayDouble if arrayDouble == classOf[Array[Double]] => {

        val extractors= cols.map(colInfo => SparkRepresentationHandler.doubleExtractor(colInfo.getType)).toArray

        SparkRepresentationHandler.getRDDArrayDouble(rdd, extractors, numCols)
      }

      case _ => throw new Exception("elementType not supported")
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

	override def cleanup = {
		// Make sure we unpersist all RDDs we own
		uncacheAll()
		super.cleanup()
	}

	private def forAllReps[T](f: RDD[_] ⇒ Any) {
		mReps.foreach {
			kv ⇒ if (kv._2 != null) f(kv._2.asInstanceOf[RDD[_]])
		}
	}

	override def cacheAll = {
		forAllReps({ rdd: RDD[_] ⇒
			if (rdd != null) {
				LOG.info(this.getClass() + ": Persisting " + rdd)
				rdd.persist
			}
		})
	}

	override def uncacheAll = {
		forAllReps({ rdd: RDD[_] ⇒
			if (rdd != null) {
				LOG.info(this.getClass() + ": Unpersisting " + rdd)
				rdd.unpersist(false)
			}
		})
	}
}
object SparkRepresentationHandler {
  /**
   *
   */
  def getRDDArrayObject(rdd: RDD[Row], numCols: Int): RDD[Array[Object]] = {

    rdd.map{row => rowToArrayObject(row, numCols)}
  }

  def getRDDArrayDouble(rdd: RDD[Row], extractors: Array[Object => Double], numCols: Int): RDD[Array[Double]] = {

    rdd.map(row => rowToArrayDouble(row, numCols, extractors)).filter(row => row != null)
  }

  def rowToArrayObject(row: Row, numCols: Int): Array[Object] = {

    val array= new Array[Object](numCols)
    var i = 0
    while(i < numCols){
      array(i)= row.getPrimitive(i)
      i += 1
    }
    array
  }

  def rowToArrayDouble(row: Row, numCols: Int, extractors: Array[Object => Double]): Array[Double] = {

    val array= new Array[Double](numCols)
    var i = 0
    var isNull= false

    while(i < numCols){
      val obj= row.getPrimitive(i)
      if(obj == null){
        isNull = true
      } else {
        array(i)= extractors(i)(obj)
      }
      i += 1
    }
    if(isNull) null else array
  }

  def doubleExtractor(colType: ColumnType): Object => Double = colType match {

    case ColumnType.DOUBLE => {
      case obj => obj.asInstanceOf[Double]
    }

    case ColumnType.INTEGER => {
      case obj => obj.asInstanceOf[Int].toDouble
    }

    case ColumnType.STRING => {
      case _ => throw new Exception("Cannot convert string to double.")
    }
  }
}