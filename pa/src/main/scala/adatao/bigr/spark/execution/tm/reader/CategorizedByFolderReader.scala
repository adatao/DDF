package adatao.bigr.spark.execution.tm.reader

import adatao.bigr.spark.execution.tm.{TextFile,Document,TextDataSource,DirectoryDataSource}
import org.apache.hadoop.fs.FileSystem
import adatao.bigr.spark.execution.ExecutionContext
import org.apache.hadoop.fs.Path
import scala.collection.mutable.ListBuffer
import adatao.bigr.spark.execution.tm.DocumentMetaData

/**
 * A CategorizedByFolderReader object will load the data from a HDFS path with
 * the assumption that documents are grouped in categories by folders.
 * This loader is good for loading large training data set for text classification
 * in bulk.
 * 
 * @author Cuong Kien Bui
 * @version
 */
class CategorizedByFolderReader(val context: ExecutionContext) extends CorpusReader {
  override def loadData(dataSource: TextDataSource): Array[Document] = {
    if (!dataSource.isInstanceOf[DirectoryDataSource]) throw new IllegalArgumentException
    val fs1 = FileSystem.get(context.hadoopConfig)
    val status = fs1.listStatus(new Path(dataSource.asInstanceOf[DirectoryDataSource].sourcePath))
    val sc = context.sparkThread.getSparkContext
    val buf = new ListBuffer[TextFile]
    for (s <- status) {
      if (s.isDir()) {
        val fileList = fs1.listStatus(s.getPath())
        for (fileStatus <- fileList) {
          val textFile = new TextFile(fileStatus.getPath().toString(), sc.textFile(fileStatus.getPath().toString()))
          textFile.metaData = new DocumentMetaData(title = fileStatus.getPath().getName(), 
              category = s.getPath().getName())
          buf += textFile
        }
      }
      // yield new TextFile(s.getPath.toString(), sc.textFile(s.getPath.toString))
    }
    return buf.sortBy(_.path).toArray
  }
}
