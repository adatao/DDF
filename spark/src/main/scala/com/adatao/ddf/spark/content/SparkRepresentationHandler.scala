/**
 *
 */
package com.adatao.ddf.spark.content

import com.adatao.ddf.content.{IHandleRepresentations, ARepresentationHandler}
import com.adatao.ddf.content.Schema.ColumnType
import com.adatao.ddf.spark.etl.SparkPersistenceHandler
import java.lang.Class
import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import scala.reflect.Manifest
import com.adatao.ddf.spark.{SparkDDFManager}
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import shark.memstore2.TablePartition
import shark.api.{Row, TableRDD}
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

    val reprHandler= container.getRepresentationHandler

    if(reprHandler.get(classOf[TablePartition]) == null){
      throw new Exception("Please load container representation with sqlLoad")
    }

    val columnNames= cols.map{col => col.getName}

    val cmd = "select " + columnNames.mkString(", ") + " from " + schema.getTableName()

    val tableRDD: TableRDD= container.getPersistenceHandler.asInstanceOf[SparkPersistenceHandler].sql2rdd(cmd)

    elementType match {

      case arrayObject if arrayObject == classOf[Array[Object]] => SparkRepresentationHandler.getRDDArrayObject(tableRDD)

      case arrayDouble if arrayDouble == classOf[Array[Double]] => {

        val extractors= cols.map(colInfo => SparkRepresentationHandler.doubleExtractor(colInfo.getType)).toArray

        SparkRepresentationHandler.getRDDArrayDouble(tableRDD, extractors)
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
  def getRDDArrayObject(tableRDD: TableRDD): RDD[Array[Object]] = {

    tableRDD.map{row => rowToArrayObject(row, tableRDD.schema.length)}
  }

  def getRDDArrayDouble(tableRDD: TableRDD, extractors: Array[Object => Double]): RDD[Array[Double]] = {

    tableRDD.map(row => rowToArrayDouble(row, tableRDD.schema.length, extractors)).filter(row => row != null)
  }

  def rowToArrayObject(row: Row, length: Int): Array[Object] = {

    val array= new Array[Object](length)
    var i = 0
    while(i < length){
      array(i)= row.getPrimitive(i)
      i += 1
    }
    array
  }

  def rowToArrayDouble(row: Row, length: Int, extractors: Array[Object => Double]): Array[Double] = {

    val array= new Array[Double](length)
    var i = 0
    var isNull= false

    while(i < length){
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