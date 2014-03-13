package adatao.bigr.spark.execution

import adatao.ML.{TModel, ALinearModel, TPredictiveModel}
import adatao.ML.types.Vector
import adatao.ML.spark.{Metrics, RddUtils}
import org.apache.spark.rdd.RDD
import adatao.bigr.spark.DataManager
import adatao.bigr.spark.DataManager.DataContainer.ContainerType

/**
 * Compute the confusion matrix for a binary classification model, given a threshold.
 * The given model should be able to predict y such that 0 <= y <= 1.
 * @author aht
 */
class BinaryConfusionMatrix(dataContainerID: String, val modelID: String, val xCols: Array[Int], val yCol: Int, val threshold: Double) extends AExecutor[BinaryConfusionMatrixResult] {

	override def runImpl(ctx: ExecutionContext): BinaryConfusionMatrixResult = {
		// first, compute RDD[(ytrue, ypred)]
		val predictions = getYtrueYpred(dataContainerID, modelID, xCols, yCol, ctx)

		// then compute confusion matrix
		val cm = Metrics.binaryConfusionMatrix(predictions, threshold)

		new BinaryConfusionMatrixResult(cm(3), cm(1), cm(2), cm(0))
	}
}

class BinaryConfusionMatrixResult(val truePos: Long, val falsePos: Long, val falseNeg: Long, val trueNeg: Long)
