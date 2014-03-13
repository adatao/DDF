package adatao.bigr.spark.execution.tm

import adatao.ML.types.TJsonSerializable
import adatao.bigr.spark.execution.ExecutionContext
import adatao.bigr.spark.execution.AExecutor
import adatao.bigr.spark.execution.tm.TextCategorizationTrainer.TextCategorizationTrainerResult
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.math._
import adatao.ML.tm.{NaiveBayesModel, CategoryModel, ATextCategorizationModel}

/**
 * An executor to train a text classifier from a given training data set (corpus).
 * The classifier specifies the algorithm to be used, e.g. NaiveBayes, kNN, SVM.
 * 
 * @author Cuong Kien Bui
 * @version 1.0
 */
class TextClassificationTrainer(val dataContainerID: String, val classifier: String) extends AExecutor[TextCategorizationTrainerResult](doPersistResult = true) {
  protected override def runImpl(context: ExecutionContext): TextCategorizationTrainerResult = {
    LOG.info("Training with the corpus: " + dataContainerID);
    val dCorpus = context.sparkThread.getDataManager.getObject(dataContainerID).asInstanceOf[DistributedCorpus]
    
    if (classifier != "NaiveBayes") throw new IllegalArgumentException("Only NaiveBayes classifier is supported.")
    
    val categoriesTable = new HashMap[String, ListBuffer[String]]
    for (doc <- dCorpus.dataset) {
      val txtFile = doc.asInstanceOf[TextFile]
      var catFileListOp = categoriesTable.get(txtFile.metaData.category)
      if (catFileListOp.isEmpty) {
        categoriesTable.put(txtFile.metaData.category, new ListBuffer[String])
        catFileListOp = categoriesTable.get(txtFile.metaData.category)
      }
      catFileListOp.get += txtFile.path
    }
    val sc = context.sparkThread.getSparkContext
    // Now load the whole categories
    val catRDD = new HashMap[String, RDD[String]]
    for (cat <- categoriesTable.keySet) {
      val buf = categoriesTable(cat)
      val itr = buf.iterator
      val firstStr = itr.next
      val pathLst = new StringBuilder
      pathLst.append(firstStr)
      while (itr.hasNext) {
        pathLst.append("," + itr.next)
      }
      catRDD.put(cat, sc.textFile(pathLst.toString))
    }
    val naiveBayesModel = new HashMap[String, CategoryModel]
    val vocab = new HashSet[String]
    for (cat <- catRDD.keySet) {
      var tmpRDD = catRDD(cat)
      // Now apply all the transformations of the corpus
      // to original file
      for (trans <- dCorpus.transformationList) {
        tmpRDD = trans match {
          case "removeExtraWhitespace" => tmpRDD.map(line => line.replaceAll("(\\s)+", " "))
          case "toLowerCase" => tmpRDD.map(line => line.toLowerCase)
          case _ => throw new IllegalArgumentException("Unsupport operator " + trans)
        }
      }
      val col = tmpRDD.flatMap(line => {
            val tmp = line.toLowerCase().split(" ") 
            for (w <- tmp if w.length > 0) yield w
          }
          ).map(word => (word, 1L)).reduceByKey((a, b) => a + b)
      val termFreqs = col.collect
      val termFreqMap = new HashMap[String, Long]
      var totalTerms: Long = 0
      for (p <- termFreqs) {
        // println(p)
        termFreqMap.put(p._1, p._2)
        vocab += p._1
        totalTerms += p._2
      }
      val catPrior:Double = categoriesTable(cat).size.doubleValue/dCorpus.dataset.length
      val catModel = new CategoryModel(catPrior, totalTerms, termFreqMap)
      naiveBayesModel.put(cat, catModel)
      LOG.info("totalTerms = " + totalTerms)
    }
    val model = new NaiveBayesModel(vocab.size, naiveBayesModel)
    LOG.info("Vocab size:" + vocab.size)
    
    LOG.info("Got content:")
    val tmpuid = context.sparkThread.getDataManager.putObject(model)
    model.uid = tmpuid
    new TextCategorizationTrainerResult(tmpuid)
  }
}



object TextCategorizationTrainer extends Serializable {
  class TextCategorizationTrainerResult(val dataContainerID: String) 
      extends TJsonSerializable {
    def this() {
      this(null)
    }
  }
}

// text predictor can't use normal pattern of APredictionExecutor
// the we need the execurtion context to read the data which might
// be very big, might not fit into a single machine memory
class TextCategorizationPredictor(val modelDataContainerID: String, val corpusDataContainerID: String, val testDocuments: Array[Int]) extends AExecutor[Array[String]] {
  def this(modelDataContainerID: String, corpusDataContainerID: String, testDocuments: Int) {
    this(modelDataContainerID, corpusDataContainerID, Array(testDocuments))
  }
  protected override def runImpl(context: ExecutionContext): Array[String] = {
    val dCorpus = context.sparkThread.getDataManager.getObject(corpusDataContainerID).asInstanceOf[DistributedCorpus]
    val model = context.sparkThread.getDataManager.getObject(modelDataContainerID).asInstanceOf[ATextCategorizationModel]
    LOG.info("Found the corpus " + corpusDataContainerID)
    // LOG.info(dCorpus)
    LOG.info("Found the model " + modelDataContainerID)
    
    return model.predict((dCorpus, testDocuments))
    
  }
}
