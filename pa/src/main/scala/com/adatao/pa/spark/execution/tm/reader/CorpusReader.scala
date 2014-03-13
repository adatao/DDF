package adatao.bigr.spark.execution.tm.reader

import adatao.bigr.spark.execution.tm.{Document, TextDataSource}

/**
 * An abstract class for all kinds of corpus reader.
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 */
abstract class CorpusReader {
  def loadData(dataSource: TextDataSource): Array[Document]
}
