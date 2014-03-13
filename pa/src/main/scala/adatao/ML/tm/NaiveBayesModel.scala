package adatao.ML.tm

import scala.collection.mutable.HashMap
import adatao.bigr.spark.execution.tm.DistributedCorpus
import scala.collection.mutable.ListBuffer
import scala.math._

/**
 * @author Cuong Kien Bui
 * @version 0.1
 */
class CategoryModel(val prior: Double, val totalTermsCount: Long, 
    val termFreqMap: HashMap[String, Long]) extends Serializable {
}

/**
 * @author Cuong Kien Bui
 * @version 0.1
 */
class NaiveBayesModel(dataContainerID: String, val vocabSize: Long, 
    val catDataModel: HashMap[String, CategoryModel]) extends ATextCategorizationModel {
  var uid: String = dataContainerID
  
  def this(vocabSize: Long, catDataModel: HashMap[String, CategoryModel]) {
    this(null, vocabSize, catDataModel)
  }
  
  /**
   * performs text categorization based on NaiveBayes algorithm.
   * 
   * @param input The testing corpus and the list of documents to be tested.
   * @return an array of string where each element is the class label for the
   * corresponding test document.
   */
  def predict(input: (DistributedCorpus, Array[Int])): Array[String] = {
    val dCorpus = input._1
    val testDocuments = input._2
    
    val result = new Array[String](testDocuments.length)
    for (i <- 0 to testDocuments.length - 1) {
      val txtFile = dCorpus.dataset(testDocuments(i))
      val catProb = new ListBuffer[(String, Double)]
      for (cat <- catDataModel.keySet) {
        val catModel = catDataModel(cat)
        val denom: Double = catModel.totalTermsCount
        val prob = txtFile.data.flatMap(line => {
            val tmp = line.toLowerCase().split(" ")
            for (w <- tmp if w.length > 0) yield w
          }
        ).map(word => {
          val freq = catModel.termFreqMap.get(word)
          if (freq.isEmpty) log(1/denom)
          else log(freq.get / denom)
        }).reduce((a, b) => a + b)
        catProb += ((cat, prob + log(catModel.prior)))
      }
      // find the cat with maximum prob
      val iter = catProb.iterator
      var bestPair = iter.next
      while (iter.hasNext) {
        val tmpPair = iter.next
        if (tmpPair._2 > bestPair._2) bestPair = tmpPair
      }
      result(i) = bestPair._1
    }
    return result
  }
}