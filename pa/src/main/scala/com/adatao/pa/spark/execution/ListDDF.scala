package com.adatao.pa.spark.execution

//import com.adatao.pa.spark.execution.ListDDF.DDFInformation
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import io.ddf.DDF.DDFInformation

/**
 * author: daoduchuan
 */
class ListDDF extends AExecutor[Array[DDFInformation]] {

  override def runImpl(context: ExecutionContext): Array[DDFInformation] = {
    val manager = context.sparkThread.getDDFManager
    manager.listDDFs()
  }
}

