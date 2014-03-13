package adatao.bigr.spark.execution.tm

import org.apache.spark.rdd.RDD

/**
 * A DistributedDocumentTermMatrix object contains a sparse matrix representation
 * for the document term matrix of a corpus.
 * 
 * An important assumption here is the number of documents is much smaller
 * than the number of words in the corpus to make this representation efficient. 
 * Usually it should be less than 100K of documents.
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 */
class DistributedDocumentTermMatrix(dataContainerID: String, val corpusID: String, val sparseMatrix: Array[RDD[(String, Int)]]) {
  var uid: String = dataContainerID
  
  def this(corpusID: String, sparseMatrix: Array[RDD[(String, Int)]]) {
    this(null, corpusID, sparseMatrix)
  }
}