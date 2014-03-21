package com.adatao.spark.ddf.analytics

import com.adatao.ddf.scalatypes.Matrix
import com.adatao.ddf.scalatypes.Vector
import com.adatao.spark.ddf.analytics._
import scala.util.Random
import org.jblas.MatrixFunctions
import org.jblas.DoubleMatrix
import org.apache.spark.rdd.RDD
import com.adatao.ddf.scalatypes._

class LogisticRegressionCRS {

}

object LogisticRegressionCRS {
	/**
   * This is the signature to be used by clients that can represent their data using [[Matrix]] and [[Vector]]
   */
  
  def train[XYDataType](
    lossFunction: ALinearGradientLossFunction[XYDataType],
    numIters: Int,
    learningRate: Double,
    initialWeights: Vector,
    numFeatures: Int /* used only if initialWeights is null */ )(implicit m: Manifest[XYDataType]): LogisticRegressionModel = {

    val (finalWeights, trainingErrors, numSamples) = GradientDescent.runBatch(lossFunction, getWeights(Option(initialWeights), numFeatures), learningRate, numIters)
    new LogisticRegressionModel(finalWeights, trainingErrors, numSamples)
  }
  
  
  def train(XYData: RDD[TupleMatrixVector],
		numIters: java.lang.Integer,
    learningRate: java.lang.Double,
    ridgeLambda: java.lang.Double,
    initialWeights: scala.Array[Double],
    numFeatures: Int): LogisticRegressionModel = {

	  
	  val snumIters: Int = numIters.asInstanceOf[Int]
	  val slearningRate: Double = learningRate.asInstanceOf[Double]
	  val sridgeLambda: Double = ridgeLambda.asInstanceOf[Double]
	  val snumFeatures: Int = numFeatures.asInstanceOf[Int]
	  
	  val lossFunction = new LossFunction(XYData, ridgeLambda)
	  var a: Vector = null
	   
	  if(initialWeights != null && initialWeights.size > 0) {
  	  var i = 0
  	  a = new Vector(initialWeights.size) 
  	  while(i < initialWeights.size) {
  	 	  a.put(i, initialWeights(i))
  	 	  i += 1
  	  }
	  }
	  
    //depend on length of weights
    val model: LogisticRegressionModel = train(lossFunction, snumIters, slearningRate, a, snumFeatures)
    
    println(">>>>>>>>>>>> model=" + model.toString() )

    model
  }
  
  
  private def getWeights(initialWeights: Option[Vector], numFeatures: Int /* used only if initialWeights is null */ ): Vector = {
    initialWeights match {
      case Some(vector) ⇒ vector
      case None ⇒ Vector(Seq.fill(numFeatures)(Random.nextDouble).toArray)
    }
  }
}

class LogisticRegressionModel(weights: Vector, trainingLosses: Vector, numSamples: Long) {
	
	override def toString(): String = {
		weights.toString + "\t" + trainingLosses.toString() + "\t" + numSamples
	}
  def predict(features: Array[Double]): Double = 0.0
}

object GradientDescent  {

  def runBatch[XYDataType](
    lossFunction: ALossFunction,
    initialWeights: Vector,
    learningRate: Double,
    numIters: Int): (Vector, Vector, Long) = {

    // Track training errors for all iterations, plus 1 more for the error before the first iteration
    val trainingLosses = new Vector(numIters + 1)
    val weights = initialWeights.dup

    var iter = 0
    var computedLoss: ALossFunction = null
    while (iter < numIters + 1) {
      
      computedLoss = lossFunction.compute(weights)
      
      // Update the weights, except for the last iteration
      // weights = weights - alpha * averageGradient
      if (iter <= numIters) weights.subi(computedLoss.gradients.mul(learningRate / computedLoss.numSamples))
      trainingLosses.put(iter, computedLoss.loss / computedLoss.numSamples)
      
      println(">>>>>> trainingLoss(" + iter + ")=" + trainingLosses.get(iter))

      iter += 1
    }

    
    //    LOG.info("final weights = %s".format(weights.toString))
    //    LOG.info("trainingLosses = %s".format(trainingLosses.toString))

    (weights, trainingLosses, computedLoss.numSamples)
  }
}

//class LossFunction(XYData: (Matrix, Vector), ridgeLambda: Double) extends ALogisticGradientLossFunction(XYData, ridgeLambda) {
//    override def compute: Vector ⇒ ALossFunction = {
//      (weights: Vector) ⇒ this.compute(XYData._1, XYData._2, weights)
//    }
//}

class LossFunction(@transient XYData: RDD[TupleMatrixVector], ridgeLambda: Double) extends ALogisticGradientLossFunction(XYData, ridgeLambda) {
    def compute: Vector ⇒ ALossFunction = {
//      (weights: Vector) ⇒ XYData.map { case (x,y) ⇒ this.compute(x, y, weights) }.reduce(_.aggregate(_))
      (weights: Vector) ⇒ XYData.map { case a ⇒ this.compute(a.x, a.y, weights) }.reduce(_.aggregate(_))
    }
}

/**
 * Useful functions such as sigmoid() are defined here
 */

object ALossFunction {
  def sigmoid(z: DoubleMatrix): Vector = Vector(MatrixFunctions.expi(z.neg).addi(1.0)).reciprocal
  def sigmoid(z: Vector): Vector = Vector(MatrixFunctions.expi(z.neg).addi(1.0)).reciprocal
  def sigmoid(z: Double): Double = 1 / (math.exp(-z) + 1)

  /**
   * Avoid over/underflow of log(sigmoid(x)) for very large values x < -710 or x > 710.
   * We substitute overflowed valeus by appropriate given limits (given by caller).
   */
  def safeLogOfSigmoid(hypothesis: DoubleMatrix, subtituteIfInfinity: DoubleMatrix) = {
    var result = MatrixFunctions.log(hypothesis)
    var i = 0
    while (i < hypothesis.length) {
      if (result.get(i).isInfinity) result.put(i, subtituteIfInfinity.get(i))
      i += 1
    }

    result
  }
}

/**
 * ALossFunction is a class with methods defined to compute loss surfaces
 * and their gradients.
 *
 * Note that the losses (J) are computed only for records and analysis.
 * If we wanted to be even faster we could skip computing losses altogether,
 * and focus only on the gradients needed to update the weights.
 */
abstract class ALossFunction extends Serializable {
  var gradients: Vector = _
  var loss: Double = 0
  var weights: Vector = _
  var numSamples: Long = 0

  /**
   * Simply returns the client-provided XYData, if ever needed
   */
  //  def getXYData: XYDataType = XYData

  /**
   * Sum in-place to avoid new object alloc.
   *
   * May override this to define an associative-commutative summing function that
   * aggregates multiple ALossFunction into one.
   *
   */
  def aggregate(other: ALossFunction): ALossFunction = {
    gradients.addi(other.gradients)
    loss += other.loss
    numSamples += other.numSamples
    this
  }

  /**
   * Must override this to compute the gradient for a given X (features),Y (output) data table, and input weights.
   *
   * @param X - Matrix of input features. Row matrix of size [m x n] where m = number of samples, and n is number of features.
   * @param Y - Output values.
   * @param weights - Column vector containing weights for every feature.
   */
  def compute(X: Matrix, Y: Vector, weights: Vector): ALossFunction

  /**
   * Must override this to define a hyper-function that calls [[this.compute(X, Y, weights)]] while transforming
   * the input data XYData: XYDataType into the appropriate X: Matrix and Y: Vector.
   *
   * This is the key to decoupling of the algorithm from the execution environment's data representation, since
   * [[XYDataType]] is parameterized. Each implementation can provide its own [[ALossFunction]] that knows how to take
   * source data of type [[XYDataType]], and provides a way to compute the loss function value out of it.
   * See, e.g., [[com.adatao.ML.LinearRegression.LossFunction]] and compare it with
   * [[com.adatao.pa.spark.execution.LinearRegression.LossFunction]].
   */
  def compute: Vector ⇒ ALossFunction
}

abstract class R[XYDataType](@transient XYData: XYDataType, ridgeLambda: Double)
    extends ALinearGradientLossFunction[XYDataType](XYData, ridgeLambda) {

  /**
   * Override to apply the sigmoid function
   *
   * hypothesis[vector] = sigmoid(weights*X)
   */
  override def computeHypothesis(X: Matrix, weights: Vector): (DoubleMatrix, DoubleMatrix) = {
    val linearPredictor = this.computeLinearPredictor(X, weights)
    (linearPredictor, ALossFunction.sigmoid(linearPredictor))
  }

  // LogisticRegression gradients is exactly the same as LinearRegression's

  /**
   * Override to compute the appropriate loss function for logistic regression
   *
   * h = hypothesis
   * J[scalar] = -(Y'*log(h) + (1-Y')*log(1-h)) + (lambda*weights^2 / 2)
   */
  override def computeLoss(X: Matrix, Y: Vector, weights: Vector, errors: DoubleMatrix, linearPredictor: DoubleMatrix, hypothesis: DoubleMatrix) = {
    /**
     * We have
     *   a1. lim log(sigmoid(x)) = 0 as x goes to +infinity
     *   a2. lim log(sigmoid(x)) = -x as x goes to -infinity
     * Likewise, 
     *   b1. lim log(1-sigmoid(x)) = -x as x goes to +infinity
     *   b2. lim log(1-sigmoid(x)) = 0 as x goes to -infinity
     *
     * We calculate h = sigmoid(x) under floating point arithmetic,
     * then we calculate log(h) and 1-log(h), substituting overflowed values.
     *
     * The behavior of cases a1 and b2  hold under floating point arithmetic, i.e.
     *   a1. when x > 720, h = 1, log h = 0
     *   b2. when x < -720, h = 0, 1-h) = 1, log (1-h) = 0
     * The other two cases result in overflow:
     *   a2. when x < -720, h = 0, log h = -Infinity => replace with x
     *   b1. when x > 720, h = 1, (1-h) = 0, log (1-h) = -Infinity => replace with -x
     *
     * This is actually quite complicated for a misleadingly-simple-looking few lines of code.
     */
    val YT = Y.transpose()
    val lossA: Double = Y.dot(ALossFunction.safeLogOfSigmoid(hypothesis, linearPredictor)) // Y' x log(h)
    val lossB: Double = Vector.fill(Y.length, 1.0).subi(Y).dot(
      ALossFunction.safeLogOfSigmoid(Vector.fill(Y.length, 1.0).subi(hypothesis), linearPredictor.neg)
    ) // (1-Y') x log(1-h)
    var J = -(lossA + lossB)
    if (ridgeLambda != 0.0) J += (ridgeLambda / 2) * weights.dot(weights)
    J
  }
}

abstract class ALinearGradientLossFunction[XYDataType](@transient XYData: XYDataType, ridgeLambda: Double)
    extends ALossFunction {
  
  final def computeLinearPredictor(X: Matrix, weights: Vector): DoubleMatrix = X.mmul(weights)
  
  /**
   * May override this to define a hypothesis function. The base implementation is
   *
   * hypothesis[vector] = weights*X
   * 
   * @returns - tuple of (linearPredictor, hypothesis), as they may be different
   */
  protected def computeHypothesis(X: Matrix, weights: Vector): (DoubleMatrix, DoubleMatrix) = {
    val linearPredictor = this.computeLinearPredictor(X, weights)
    (linearPredictor, linearPredictor)
  }

  /**
   * May override this to define a specific gradient (dJ/dWeights). The base implementation
   * computes the linear gradients with ridge regularization.
   *
   * errors = hypothesis - Y
   * totalGradients[vector] = errors*X + lambda*weights
   */
  protected def computeGradients(X: Matrix, weights: Vector, errors: DoubleMatrix): Vector = {
    val gradients = Vector(errors.transpose().mmul(X)) // (h - Y) x X = errors.transpose[1 x m] * X[m x n] = [1 x n] => Vector[n]
    if (ridgeLambda != 0.0) gradients.addi(weights.mul(ridgeLambda)) // regularization term, (h - Y) x X + L*weights
    gradients
  }

  /**
   * May override this to define a specific loss (J) function. The base implementation
   * computes the linear loss function with ridge regularization.
   *
   * J[scalar] = errors^2 + lambda*weights^2
   */
  protected def computeLoss(X: Matrix, Y: Vector, weights: Vector, errors: DoubleMatrix, linearPredictor: DoubleMatrix, hypothesis: DoubleMatrix): Double = {
    var J = errors.dot(errors)
    if (ridgeLambda != 0.0) J += ridgeLambda * weights.dot(weights)
    J / 2
  }

  /**
   * Note that the losses are computed only for records and analysis. If we wanted to be even faster
   * we could skip computing losses altogether.
   */
  override def compute(X: Matrix, Y: Vector, theWeights: Vector): ALossFunction = {
    val (linearPredictor, hypothesis) = this.computeHypothesis(X, theWeights)
    val errors = hypothesis.sub(Y)
    gradients = this.computeGradients(X, theWeights, errors)
    loss = this.computeLoss(X, Y, theWeights, errors, linearPredictor, hypothesis)
    weights = theWeights
    numSamples = Y.rows

    this
  }
}

abstract class ALogisticGradientLossFunction[XYDataType](@transient XYData: XYDataType, ridgeLambda: Double)
    extends ALinearGradientLossFunction[XYDataType](XYData, ridgeLambda) {

  /**
   * Override to apply the sigmoid function
   *
   * hypothesis[vector] = sigmoid(weights*X)
   */
  override def computeHypothesis(X: Matrix, weights: Vector): (DoubleMatrix, DoubleMatrix) = {
    val linearPredictor = this.computeLinearPredictor(X, weights)
    (linearPredictor, ALossFunction.sigmoid(linearPredictor))
  }

  // LogisticRegression gradients is exactly the same as LinearRegression's

  /**
   * Override to compute the appropriate loss function for logistic regression
   *
   * h = hypothesis
   * J[scalar] = -(Y'*log(h) + (1-Y')*log(1-h)) + (lambda*weights^2 / 2)
   */
  override def computeLoss(X: Matrix, Y: Vector, weights: Vector, errors: DoubleMatrix, linearPredictor: DoubleMatrix, hypothesis: DoubleMatrix) = {
    /**
     * We have
     *   a1. lim log(sigmoid(x)) = 0 as x goes to +infinity
     *   a2. lim log(sigmoid(x)) = -x as x goes to -infinity
     * Likewise, 
     *   b1. lim log(1-sigmoid(x)) = -x as x goes to +infinity
     *   b2. lim log(1-sigmoid(x)) = 0 as x goes to -infinity
     *
     * We calculate h = sigmoid(x) under floating point arithmetic,
     * then we calculate log(h) and 1-log(h), substituting overflowed values.
     *
     * The behavior of cases a1 and b2  hold under floating point arithmetic, i.e.
     *   a1. when x > 720, h = 1, log h = 0
     *   b2. when x < -720, h = 0, 1-h) = 1, log (1-h) = 0
     * The other two cases result in overflow:
     *   a2. when x < -720, h = 0, log h = -Infinity => replace with x
     *   b1. when x > 720, h = 1, (1-h) = 0, log (1-h) = -Infinity => replace with -x
     *
     * This is actually quite complicated for a misleadingly-simple-looking few lines of code.
     */
    val YT = Y.transpose()
    val lossA: Double = Y.dot(ALossFunction.safeLogOfSigmoid(hypothesis, linearPredictor)) // Y' x log(h)
    val lossB: Double = Vector.fill(Y.length, 1.0).subi(Y).dot(
      ALossFunction.safeLogOfSigmoid(Vector.fill(Y.length, 1.0).subi(hypothesis), linearPredictor.neg)
    ) // (1-Y') x log(1-h)
    var J = -(lossA + lossB)
    if (ridgeLambda != 0.0) J += (ridgeLambda / 2) * weights.dot(weights)
    J
  }
}

