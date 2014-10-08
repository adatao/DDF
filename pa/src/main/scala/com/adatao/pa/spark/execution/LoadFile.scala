package com.adatao.pa.spark.execution

import com.adatao.pa.spark.Utils.DataFrameResult
import io.ddf.content.Schema
import com.adatao.spark.content.LoadFileUtils
import io.spark.ddf.SparkDDFManager

/**
 */
class LoadFile
  (fileURL: String,
   schemaString: String, separator: String)
  extends AExecutor[DataFrameResult](false) {

  override def runImpl(context: ExecutionContext): DataFrameResult = {
    val schema = new Schema(schemaString)
    val manager = context.sparkThread.getDDFManager.asInstanceOf[SparkDDFManager]
    val ddf = LoadFileUtils.loadFile(manager, fileURL, schema, separator)
    manager.addDDF(ddf)
    new DataFrameResult(ddf)
  }
}
