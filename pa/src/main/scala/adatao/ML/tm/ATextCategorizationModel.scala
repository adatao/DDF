package adatao.ML.tm

import adatao.bigr.spark.execution.tm.DistributedCorpus
import adatao.ML.TPredictiveModel

/**
 * @author Cuong Kien Bui
 * @version 0.1
 */
abstract class ATextCategorizationModel extends TPredictiveModel[(DistributedCorpus, Array[Int]), Array[String]]{
  def predict(input: (DistributedCorpus, Array[Int])): Array[String]
}