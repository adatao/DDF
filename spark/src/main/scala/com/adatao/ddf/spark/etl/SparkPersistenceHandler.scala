package com.adatao.ddf.spark.etl

import com.adatao.ddf.DDF
import com.adatao.ddf.spark.{SparkDDFManager}
import com.adatao.ddf.spark.content.SparkRepresentationHandler
import org.apache.spark.rdd.RDD
import com.adatao.ddf.etl.APersistenceHandler
import shark.SharkContext
import shark.api.TableRDD
import shark.memstore2.TablePartition
import com.adatao.ddf.content.{Schema}
import com.adatao.ddf.content.Schema.ColumnType

class SparkPersistenceHandler(container: SparkDDFManager, var sharkContext: SharkContext= null) extends APersistenceHandler(container) {

  def sql2rdd(command: String): TableRDD = {

    sharkContext.sql2rdd(command)
  }

  override def sqlLoad(command: String): Unit = {
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
          case "int" => ColumnType.INTEGER
          case "double" => ColumnType.DOUBLE
          case "string" => ColumnType.STRING
        }
        val column= new Schema.Column(col.columnName, colType)
        container.getMetaDataHandler.getSchema.addColumn(column)
      }
    }

		this.getDDF().getDDFManager().getRepresentationHandler().asInstanceOf[SparkRepresentationHandler].set(rdd)
	}

  def sql(command: String): Array[String]= {
    sharkContext.sql(command).toArray
  }
	override def sqlSave(connection: String, command: String): Unit = { null }

	//override def jdbcLoad(connection: String, command: String): DDF = { null }

	//override def jdbcSave(connection: String, command: String): Unit = { null }
}