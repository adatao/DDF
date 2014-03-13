package com.adatao.pa.spark.execution.tm

import com.adatao.ML.types.TJsonSerializable
import com.adatao.pa.spark.execution.ExecutionContext
import com.adatao.pa.spark.execution.AExecutor
import com.adatao.pa.spark.execution.tm.InspectDistributedCorpus.InspectDistributedCorpusResult

/**
 * An executor that inspects a subset of a distributed corpus with the uuid (dataContainerID),
 * returns a list of document samples along with document meta data.
 * This action performs a collect (take) operation so it could be expensive.
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 */
class InspectDistributedCorpus(val dataContainerID: String, val inspectRange: Array[Int]) extends AExecutor[InspectDistributedCorpusResult] {
  def this(dataContainerID: String, inspectRange: Int) {
    this(dataContainerID, Array(inspectRange))
  }
  protected override def runImpl(context: ExecutionContext): InspectDistributedCorpusResult = {
    LOG.info("Inspecting the corpus: " + dataContainerID);
    val dCorpus = context.sparkThread.getDataManager.getObject(dataContainerID).asInstanceOf[DistributedCorpus]
    
    val metaData = new Array[DocumentMetaData](inspectRange.length)
    val content = new Array[String](inspectRange.length)
    
    for (i <- 0 to inspectRange.length - 1) {
      metaData(i) = dCorpus.dataset(inspectRange(i)).metaData
      val tmpContent = dCorpus.dataset(inspectRange(i)).data.take(10)
      val buffer = new StringBuilder
      for (line <- tmpContent) {
        buffer.append(line)
        buffer.append("\n")
      }
      content(i) = buffer.toString
    }
    new InspectDistributedCorpusResult(metaData, content)
  }
}

object InspectDistributedCorpus extends Serializable {
  class InspectDistributedCorpusResult(val metaData: Array[DocumentMetaData], val content: Array[String]) 
      extends TJsonSerializable {
    
  }
}
