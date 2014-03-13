package com.adatao.pa.spark.execution.tm

import com.adatao.ML.types.TJsonSerializable
import com.adatao.pa.spark.execution.ExecutionContext
import com.adatao.pa.spark.execution.AExecutor
import com.adatao.pa.spark.execution.tm.InspectDocumentTermMatrix.InspectDocumentTermMatrixResult
import org.apache.spark.SparkContext._
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap

/**
 * An executor to inspect the content of a sparse matrix representing the
 * document term matrix (given by the dataContainerID).
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 */
class InspectDocumentTermMatrix(val dataContainerID: String, val documents: Array[Int], val terms: Array[String]) extends AExecutor[InspectDocumentTermMatrixResult] {
  protected override def runImpl(context: ExecutionContext): InspectDocumentTermMatrixResult = {
    LOG.info("Inspecting the corpus: " + dataContainerID);
    val dtm = context.sparkThread.getDataManager.getObject(dataContainerID).asInstanceOf[DistributedDocumentTermMatrix]
    val dcorpus = context.sparkThread.getDataManager.getObject(dtm.corpusID).asInstanceOf[DistributedCorpus]
    val termSet = new HashSet[String] // Use HashSet for the best speed
    for (t <- terms) termSet +=(t.toLowerCase())
    
    val sampleMatrix = Array.ofDim[Int](documents.length, terms.length)
    for (i <- 0 to documents.length - 1) {
      val wordcount = dcorpus.dataset(documents(i)).data.flatMap(line => {
            val tmp = line.toLowerCase().split(" "); 
            for (w <- tmp if termSet.contains(w)) yield w
          }).map(word => (word, 1)).reduceByKey((a, b) => a + b)
      // the collecting result should be much smaller now,
      // since we only count the words that we care
      var tmpRow = wordcount.collect
      // TODO: optimize this step
      val tmp = new HashMap[String, Int]
      for (p <- tmpRow) {
        tmp.put(p._1, p._2)
      }
      for (j <- 0 to terms.length - 1) {
        val freq = tmp.get(terms(j))
        if (freq.isEmpty) {
          sampleMatrix(i)(j) = 0
        } else {
          sampleMatrix(i)(j) = freq.get
        }
      }
       
    }
    new InspectDocumentTermMatrixResult(sampleMatrix)
  }
}

object InspectDocumentTermMatrix extends Serializable {
  class InspectDocumentTermMatrixResult(val matrix: Array[Array[Int]]) 
      extends TJsonSerializable {
    
  }
}
