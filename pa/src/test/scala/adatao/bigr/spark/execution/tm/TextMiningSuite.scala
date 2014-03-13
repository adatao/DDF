package adatao.bigr.spark.execution.tm

import org.junit.Assert._
import adatao.bigr.spark.types.ABigRClientTest
import adatao.bigr.spark.types.ATestBase
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.deploy.{LocalSparkCluster, SparkHadoopUtil}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import adatao.bigr.spark.execution.tm.LoadDistributedCorpus.LoadDistributedCorpusResult
import adatao.bigr.spark.execution.tm.TextCategorizationTrainer.TextCategorizationTrainerResult

/**
 * @author Cuong Kien Bui
 * @version 0.1
 */
class TextMiningSuite extends ABigRClientTest {
  
  ignore("Load dCorpus from HDFS folder") {
    // val sparkContext = new SparkContext(SPARK_MASTER, "BigR", SPARK_HOME, jobJars)
    val conf = SparkHadoopUtil.get.newConfiguration()
    // conf.addResource(new Path("/home/cuongbk/Programs/hadoop-1.0.4/conf/core-site.xml"))
    println(conf.getRaw("fs.default.name"))
    val fs1 = FileSystem.get(conf)
    val hdfsPath = conf.getRaw("fs.default.name") + "tmp/ChinaCorpus"
    fs1.delete(new Path(hdfsPath), true) // clean up the previous data
    fs1.copyFromLocalFile(false, true, new Path("resources/tm/ChinaCorpus"), new Path(hdfsPath)) 
    val params = "{\"dataSourceType\": \"DirSource\", \"dataSourceParams\": {\"path\": \"" + hdfsPath + "\"}, \"readerType\": \"plain\", \"lang\": \"en\", \"categorizedBy\": \"folder\"}"
    val res = bigRClient.execute[LoadDistributedCorpusResult]("tm.LoadDistributedCorpus", params)
    assert(res.isSuccess)

    val dCorpus = res.result
    println(dCorpus.dataContainerID)
    assert(dCorpus.metaData.numDocs == 4) // there are 4 docs in the Chica Corpus
  }
  
  ignore("Test NaiveBayes classification") {
    // val sparkContext = new SparkContext(SPARK_MASTER, "BigR", SPARK_HOME, jobJars)
    val conf = SparkHadoopUtil.get.newConfiguration()
    // conf.addResource(new Path("/home/cuongbk/Programs/hadoop-1.0.4/conf/core-site.xml"))
    println(conf.getRaw("fs.default.name"))
    val fs1 = FileSystem.get(conf)
    val hdfsPath = conf.getRaw("fs.default.name") + "tmp/ChinaCorpus"
    fs1.delete(new Path(hdfsPath), true) // clean up the previous data
    fs1.copyFromLocalFile(false, true, new Path("resources/tm/ChinaCorpus"), new Path(hdfsPath)) 
    val params = "{\"dataSourceType\": \"DirSource\", \"dataSourceParams\": {\"path\": \"" + hdfsPath + "\"}, \"readerType\": \"plain\", \"lang\": \"en\", \"categorizedBy\": \"folder\"}"
    val res = bigRClient.execute[LoadDistributedCorpusResult]("tm.LoadDistributedCorpus", params)
    assert(res.isSuccess)

    val dCorpus = res.result
    println(dCorpus.dataContainerID)
    assert(dCorpus.metaData.numDocs == 4) // there are 4 docs in the Chica Corpua
    val params2 = "{\"dataContainerID\": \"" + dCorpus.dataContainerID + "\", \"classifier\": \"NaiveBayes\"}"
    val res2 = bigRClient.execute[TextCategorizationTrainerResult]("tm.TextClassificationTrainer", params2)
    assert(res2.isSuccess)
    val modelID = res2.result.dataContainerID
    val params3 = "{\"modelDataContainerID\": \"" + modelID + "\", \"corpusDataContainerID\": \"" + dCorpus.dataContainerID + "\", \"testDocuments\": [2,3]}"
    val res3 = bigRClient.execute[Array[String]]("tm.TextCategorizationPredictor", params3)
    println(res3)
    assertEquals(res3.result(0), "China")
    assertEquals(res3.result(1), "Japan")
  }
}
