package adatao.bigr.spark.execution.tm

import adatao.ML.types.TJsonSerializable
import adatao.bigr.spark.execution.ExecutionContext
import adatao.bigr.spark.execution.AExecutor
import adatao.bigr.spark.execution.tm.TransformDistributedCorpus.TransformDistributedCorpusResult
import scala.collection.mutable.ArrayBuffer

/**
 * An executor that performs transformations to a corpus including: removeExtraWhitespace,
 * toLowerCase, removeStopWords, stemming.
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 */
class TransformDistributedCorpus(dataContainerID: String, opName: String) extends AExecutor[TransformDistributedCorpusResult] {
  protected override def runImpl(context: ExecutionContext): TransformDistributedCorpusResult = {
    LOG.info("transforming the corpus: " + dataContainerID);
    val dCorpus = context.sparkThread.getDataManager.getObject(dataContainerID).asInstanceOf[DistributedCorpus]
    
    val newDataset = opName match {
      case "removeExtraWhitespace" => (for (txtFile <- dCorpus.dataset) 
              yield txtFile.clone(txtFile.data.map(line => line.replaceAll("(\\s)+", " "))))
      case "toLowerCase" => (for (txtFile <- dCorpus.dataset) 
              yield txtFile.clone(txtFile.data.map(line => line.toLowerCase)))
      case _ => throw new IllegalArgumentException("Unsupport operator " + opName)
    }
    // create a new corpus with a new transformation append to the end of 
    // the transformation list
    val newTransormList = new ArrayBuffer[String](dCorpus.transformationList.length + 1)
    newTransormList.appendAll(dCorpus.transformationList)
    newTransormList += opName
    val newCorpus = new DistributedCorpus(null, newDataset, newTransormList.toArray)
    val tmpuid = context.sparkThread.getDataManager.putObject(newCorpus) 
    new TransformDistributedCorpusResult(tmpuid)
  }
}

object TransformDistributedCorpus extends Serializable {
  class TransformDistributedCorpusResult(val dataContainerID: String) 
      extends TJsonSerializable {
    
  }
}
