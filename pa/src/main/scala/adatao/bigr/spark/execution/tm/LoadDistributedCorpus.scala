package adatao.bigr.spark.execution.tm

import adatao.ML.types.TJsonSerializable
import adatao.bigr.spark.execution.ExecutionContext
import adatao.bigr.spark.execution.AExecutor
import adatao.bigr.spark.execution.tm.LoadDistributedCorpus.LoadDistributedCorpusResult
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import adatao.bigr.spark.execution.tm.reader.CategorizedByFolderReader
import adatao.bigr.spark.execution.tm.{TextDataSource, DirectoryDataSource}
import java.util.HashMap

/**
 * An executor to load a distributed corpus from and HDFS Path. This class
 * will delegate the loading action to corresponding reader specified
 * by the readerType parameter (plain, PDF, CSV, DOC, etc).
 * The class will use HadoopConfiguration received from the execution context
 * to read the data from the HDFS.
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 */
class LoadDistributedCorpus(dataSourceType: String,
    dataSourceParams: HashMap[String, String],
    readerType: String = "plain",
    readerParams: String,
    lang: String = "en",
    categorizedBy: String = "none") extends AExecutor[LoadDistributedCorpusResult](doPersistResult = true) {
  protected override def runImpl(context: ExecutionContext): LoadDistributedCorpusResult = {
    LOG.info("Loading the corpus from: " + dataSourceType);
    LOG.info("lang = " + lang)
    LOG.info("readerType = " + readerType)
    LOG.info("categorizedBy = " + categorizedBy)
    var dataSource: TextDataSource = null
    if (dataSourceType == "DirSource") {
      dataSource = new DirectoryDataSource(dataSourceParams)
      LOG.info("path = " + dataSource.asInstanceOf[DirectoryDataSource].sourcePath)
    }
    if (readerType == "plain" && categorizedBy == "folder") {
	    val reader = new CategorizedByFolderReader(context)
	    val dataset = reader.loadData(dataSource)
	    val metaData = new DistributedCorpusMetaData(numDocs=dataset.length)
	    val dCorpus = new DistributedCorpus(metaData, dataset)
	    val tmpuid = context.sparkThread.getDataManager.putObject(dCorpus)
	    dCorpus.uid = tmpuid
	    new LoadDistributedCorpusResult(tmpuid, dCorpus.metaData)
    } else {
      throw new IllegalArgumentException("Only accept plain text and categorized by folder corpus.")
    }
  }
}

object LoadDistributedCorpus extends Serializable {
  class LoadDistributedCorpusResult(val dataContainerID: String, val metaData: DistributedCorpusMetaData) 
      extends TJsonSerializable {
      def this() {
        this(null, null)
      }
    
  }
}
