package com.adatao.ddf.etl

import com.adatao.ddf.DDF
import com.adatao.ddf.spark.DDFHelper
import com.adatao.ddf.spark.content.RepresentationHandler
import org.apache.spark.rdd.RDD

class PersistenceHandler(container: DDFHelper) extends APersistenceHandler(container) {

	override def sqlLoad(connection: String, command: String): Unit = {
		this.resetRepresentationsAndViews()
		// load the table into an RDD, then associate it with the current DDF
		//val rdd: RDD[TablePartition] = null
		val rdd: RDD[_] = null
		this.getDDF().getHelper().getRepresentationHandler().asInstanceOf[RepresentationHandler].set(rdd)
	}

	override def sqlSave(connection: String, command: String): Unit = { null }

	//override def jdbcLoad(connection: String, command: String): DDF = { null }

	//override def jdbcSave(connection: String, command: String): Unit = { null }
}