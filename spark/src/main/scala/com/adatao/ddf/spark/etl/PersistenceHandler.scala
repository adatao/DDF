package com.adatao.ddf.spark.etl

import com.adatao.ddf.DDF
import com.adatao.ddf.spark.{SparkDDFManager, DDFHelper}
import com.adatao.ddf.spark.content.RepresentationHandler
import org.apache.spark.rdd.RDD
import com.adatao.ddf.etl.APersistenceHandler
import shark.SharkContext
import shark.api.TableRDD
import shark.memstore2.TablePartition
import com.adatao.ddf.content.{Schema, ColumnType}

class PersistenceHandler(container: SparkDDFManager, var sharkContext: SharkContext= null) extends APersistenceHandler(container) {

  def sql2rdd(command: String): TableRDD = {

    sharkContext.sql2rdd(command)
  }

  override def sqlLoad(connection: String, command: String): Unit = {
    this.resetRepresentationsAndViews()

		// load the table into an RDD, then associate it with the current DDF
		//val rdd: RDD[TablePartition] = null
		val table= this.sql2rdd(command)
    val rdd: RDD[TablePartition] = table.map{
      row => row.rawdata.asInstanceOf[TablePartition]
    }

    table.schema.foreach{
      col => {
        val colType= col.typeName match {
          case "int" => ColumnType.INT
          case "double" => ColumnType.DOUBLE
          case "string" => ColumnType.STRING
        }
        val column= new Schema.Column(col.columnName, colType)
        container.getMetaDataHandler.getSchema.addColumn(column)
      }
    }

		this.getDDF().getHelper().getRepresentationHandler().asInstanceOf[RepresentationHandler].set(rdd)
	}

  def sql(connection: String, command: String): Unit= {
    sharkContext.sql(command)
  }
	override def sqlSave(connection: String, command: String): Unit = { null }

	//override def jdbcLoad(connection: String, command: String): DDF = { null }

	//override def jdbcSave(connection: String, command: String): Unit = { null }
}