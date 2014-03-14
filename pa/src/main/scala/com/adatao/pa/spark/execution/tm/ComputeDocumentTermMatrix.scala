package com.adatao.pa.spark.execution.tm

import com.adatao.ML.types.TJsonSerializable
import com.adatao.pa.spark.execution.ExecutionContext
import com.adatao.pa.spark.execution.AExecutor
import com.adatao.pa.spark.execution.tm.ComputeDocumentTermMatrix.ComputeDocumentTermMatrixResult
import org.apache.spark.SparkContext._

/**
 * A executor to compute DocumentTerm matrix from a training data set in the form
 * of a corpus.
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 */
class ComputeDocumentTermMatrix(dataContainerID: String) extends AExecutor[ComputeDocumentTermMatrixResult](doPersistResult = true) {
  protected override def runImpl(context: ExecutionContext): ComputeDocumentTermMatrixResult = {
    LOG.info("Computing DTM for the corpus: " + dataContainerID);
    val dCorpus = context.sparkThread.getDataManager.getObject(dataContainerID).asInstanceOf[DistributedCorpus]
    // convert all the words to lower case first?
    val sparseMatrix = for (textFile <- dCorpus.dataset) yield {
      textFile.data.flatMap(line => line.toLowerCase().split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    }
    val dtm = new DistributedDocumentTermMatrix(dCorpus.uid, sparseMatrix)
    val tmpuid = context.sparkThread.getDataManager.putObject(dtm)
    dtm.uid = tmpuid
    new ComputeDocumentTermMatrixResult(tmpuid)
  }
}

object ComputeDocumentTermMatrix extends Serializable {
  class ComputeDocumentTermMatrixResult(val dataContainerID: String) 
      extends TJsonSerializable {
    
  }
}