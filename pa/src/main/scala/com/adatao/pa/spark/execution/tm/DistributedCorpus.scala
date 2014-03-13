package com.adatao.pa.spark.execution.tm

import org.apache.spark.rdd.RDD
import java.util.UUID
import scala.collection.mutable.ListBuffer

/**
 * A Distributed Corpus object provides access to all the documents belong to
 * the corpus in the cluster.
 * 
 * It maintains a list of all documents belong to the corpus.
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 */
class DistributedCorpus(dataContainerID: String, val metaData: DistributedCorpusMetaData, val dataset: Array[Document], 
    val transformationList: Array[String]) {
  var uid: String = dataContainerID

  
  def this(metaData: DistributedCorpusMetaData, dataset: Array[Document], transformationList: Array[String] = Array[String]()) {
    this(null, metaData: DistributedCorpusMetaData, dataset, transformationList)
  }
  
}

/**
 * A Document object represents a document in the form of an RDD[String],
 * each string could be a line, a sentence.
 * 
 * Note: a document content could be very large, may not fit into a single machine
 * memory.
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 */
class Document(val data: RDD[String]) {
  var metaData: DocumentMetaData = null

  def clone(newData: RDD[String]): Document = {
    val newDoc = new Document(newData)
    newDoc.metaData = metaData
    return newDoc
  }
}

/**
 * A TextFile object is a type of Document which is persisted as a text file
 * in a filesystem (local or HDFS).
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 */
class TextFile(val path: String, data: RDD[String]) extends Document(data) {
  // protected val data: RDD[String];
  override def clone(newData: RDD[String]): Document = {
    val newDoc = new TextFile(path, newData)
    newDoc.metaData = metaData
    return newDoc
  }
}
