package com.adatao.pa.spark.execution.tm

import com.adatao.ML.types.TJsonSerializable

/**
 * @author Cuong Kien Bui
 * @version 0.1
 */
class DocumentMetaData(val title: String,
    val creator: String = null,
    val description: String = null,
    val date: String = null,
    val identifier: String = null,
    val category: String = null,
    val lang: String = null) extends TJsonSerializable {

}