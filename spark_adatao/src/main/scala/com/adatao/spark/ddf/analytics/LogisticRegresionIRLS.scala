package com.adatao.spark.ddf.analytics

import io.ddf.types.TupleMatrixVector
import io.ddf.types.Matrix
import io.ddf.types.Vector
import org.apache.spark.rdd.RDD
import org.jblas.DoubleMatrix
import org.jblas.MatrixFunctions
import org.jblas.Solve
import java.util.HashMap
import io.ddf.exception.DDFException
import scala.collection.mutable.ListBuffer

class LogisticRegresionIRLS {

}

object LogisticRegressionIRLS {

  /**
   * m:(r x c); d: (r x 1)
   * We transpose m and multiply with diagonal matrix with d is the diagonal
   */
  def tMmulDiagonal(m: DoubleMatrix, d: DoubleMatrix): DoubleMatrix = {
    val numRows = m.getRows
    val numCols = m.getColumns
    val ret = DoubleMatrix.ones(numCols, numRows)

    var c = 0
    while (c < numCols) {
      var r = 0
      while (r < numRows) {
        val m_val = m.get(r, c)
        ret.put(c, r, m_val * d.get(r, 0))
        r += 1
      }
      c += 1
    }
    ret
  }

  /**com.adatao.spark.ddf.analytis
   * d: (r x 1): array of diagonal elements
   * m:(r x c)
   * Multifly d with m and put result back to m
   */
  def diagonalMmul(d: DoubleMatrix, m: DoubleMatrix): DoubleMatrix = {
    val numRows = m.getRows
    val numCols = m.getColumns

    var r = 0
    while (r < numRows) {
      var c = 0
      while (c < numCols) {
        m.put(r, c, m.get(r, c) * d.get(r, 0))
        c += 1
      }
      r += 1
    }
    m
  }

  def handlePartition(lFunc: LossFunction, weights: Vector)(inputRows: Iterator[(Matrix, Vector)]): Iterator[(DoubleMatrix, DoubleMatrix, Long)] = {

    var XtWX: DoubleMatrix = null
    var XtWz: DoubleMatrix = null
    var nRows: Long = 0

    while (inputRows.hasNext && nRows == 0) {
      inputRows.next match {
        case (x, y) ⇒ {
          if (x.columns == 0 && y.columns == 0) {
            XtWX = DoubleMatrix.zeros(weights.length, weights.length)
            XtWz = DoubleMatrix.zeros(weights.length, 1)
          } else {

            // calculate number of rows
            nRows = x.getRows.toLong

            val hypothesis = lFunc.computeHypothesis(x, weights)
            val w = hypothesis._2.mul(DoubleMatrix.ones(y.length).sub(hypothesis._2))
            val wre = DoubleMatrix.ones(y.length).divi(w)
            val z = hypothesis._1.add(diagonalMmul(wre, y.sub(hypothesis._2)))
            // instead of XtW = mmulDiagonal(x.transpose(), w), we directory transpose and mmul to save memory
            val XtW = tMmulDiagonal(x, w)
            XtWX = XtW.mmul(x)
            XtWz = XtW.mmul(z)

            //println(hypothesis.toString)
            //println(w.toString())
          }

        }
      }
    }

    if (nRows == 0) {
      XtWX = DoubleMatrix.zeros(weights.length, weights.length)
      XtWz = DoubleMatrix.zeros(weights.length, 1)
    }

    Iterator((XtWX, XtWz, nRows))
  }

  def doIRLS(data: RDD[(Matrix, Vector)], initialWeights: Array[Double], nFeatures: Int, eps: Double, ridgeLambda: Double, numIters: Int): (Vector, DoubleMatrix, Array[Double], Int, Long) = {
    // The best and safe value for initial weights is zeros, thus the logistic value is 0.5
    // The reason is the logistic function is more likely to be 0 or 1 which makes its derivative 0 all the way, :((((( 
    var weights = if (initialWeights == null || initialWeights.length != nFeatures) Vector(DoubleMatrix.zeros(nFeatures)) else Vector(initialWeights)

    val lossFunction = new LossFunction(data, ridgeLambda)

    var lastDev = Double.MaxValue
    var computedLoss = lossFunction.compute(weights)
    println(computedLoss.gradients.toString)
    var currentDev = 2 * computedLoss.loss
    val numSamples: Long = computedLoss.numSamples

    if (numSamples == 0)
      throw new RuntimeException("No data to run, there is no rows, may be it is due to the null filtering process.")

    var XtWXlambda: DoubleMatrix = null

    var iter = 0
    var deviances = Array(currentDev)

    println("Iter " + iter + ": (lastDev: " + lastDev + ",currentDev: " + currentDev + "), eps: " + Math.abs((lastDev - currentDev) / currentDev))
    println("Iter " + iter + ":" + weights.toString())

    while (math.abs((lastDev - currentDev) / (math.abs(currentDev) + 0.1)) >= eps && iter < numIters) {
      val ret = data.mapPartitions(handlePartition(lossFunction, weights))
        .reduce((x, y) ⇒ (x._1.addi(y._1), x._2.addi(y._2), x._3 + y._3))

      val lastXtWXlambda = XtWXlambda
      XtWXlambda = ret._1

      if (ridgeLambda != 0) {
        XtWXlambda = XtWXlambda.addi(DoubleMatrix.eye(nFeatures).muli(ridgeLambda))
      }
      val lastWeights = weights
      println("Solve.solve")
      weights = Vector(Solve.solve(XtWXlambda, ret._2))

      lastDev = currentDev

      var computedLoss = lossFunction.compute(weights)
      currentDev = 2 * computedLoss.loss

      iter += 1
      println("Iter " + iter + ": XtWXlambda " + XtWXlambda.toString())
      println("Iter " + iter + ": XtWz " + ret._2.toString())
      println("Iter " + iter + ": (lastDev: " + lastDev + ",currentDev: " + currentDev + "), eps: " + math.abs((lastDev - currentDev) / currentDev))
      println("Iter " + iter + ":" + weights.toString())

      if (currentDev.isNaN()) {
        XtWXlambda = lastXtWXlambda
        weights = lastWeights
        iter -= 1
      } else {
        deviances :+= currentDev
      }
    }

    (weights, XtWXlambda, deviances, iter, numSamples)
  }

  def train(XYData: RDD[TupleMatrixVector],
    numIters: Int,
    eps: Double,
    ridgeLambda: Double,
    initialWeights: Array[Double], nullModel: Boolean): IRLSLogisticRegressionModel = {
    
    val numFeatures: Int = XYData.map(xy => xy._1.getColumns()).first()
    if (!nullModel) {
      XYData.cache()
      val ret = doIRLS(XYData.map { row ⇒ (row._1, row._2) }, initialWeights, numFeatures, eps, ridgeLambda, numIters)

      val invXtWX = Solve.solvePositive(ret._2, DoubleMatrix.eye(numFeatures))

      // standard errors
      val stderrs = org.jblas.MatrixFunctions.sqrt(invXtWX.diag())
      XYData.unpersist()
      return new IRLSLogisticRegressionModel(ret._1, ret._3(ret._4), 0, ret._5, numFeatures - 1, ret._4, Vector(stderrs))
    } else {
      // This is when user want the null deviance only
      // Run IRLS again to calculate nullDeviance which is deviance of null model(only the intercept)
      val nullPartition = XYData.map { row ⇒ (Matrix.ones(row._2.length), row._2) }.cache()
      //val p = nullPartition.collect()(0)
      //println(p._1.getRows())
      //println(p._2.getRows())
      val retNull = doIRLS(nullPartition, null, 1, eps, ridgeLambda, numIters)
      nullPartition.unpersist()
      return new IRLSLogisticRegressionModel(retNull._1, retNull._3(retNull._4), retNull._3(retNull._4), retNull._5, 1, retNull._4, null)
    }
  }

  class LossFunction(@transient XYData: RDD[(Matrix, Vector)], ridgeLambda: Double) extends ALogisticGradientLossFunction(XYData, ridgeLambda) {
    def compute: Vector ⇒ ALossFunction = {
      (weights: Vector) ⇒ XYData.map { case (x, y) ⇒ this.compute(x, y, weights) }.reduce(_.aggregate(_))
    }
  }
}

class IRLSLogisticRegressionModel(weights: Vector, val deviance: Double, val nullDeviance: Double, numSamples: Long,
                                  val numFeatures: Long, val numIters: Int, val stderrs: Vector) extends ALinearModel[Double](weights, numSamples) {
  override def toString(): String = {
    val weightString = s"weights: [${weights.data.mkString(", ")}]"
    val devianceString = s"deviance: ${deviance}"
    val nullDevString = s"null deviance: ${nullDeviance}"
    val stdErrsString = s"Standard Errors: [${stderrs.data.mkString(",")}]"
    this.getClass.getName + "\n" + weightString + "\n" + devianceString + "\n" + nullDevString + "\n" + stdErrsString
  }

  override def predict(features: Vector): Double = {
    ALossFunction.sigmoid(this.linearPredictor(Vector(features.data)))
  }

  override def predict(point: Array[Double]): Double = {
    val features = Vector(Array[Double](1.0) ++ point)
    if(features.size != weights.size) {
      throw new DDFException(s"error predicting, features.size = ${features.size}, weights.size = ${weights.size}")
    }
    this.predict(features)
  }

  override def yTrueYPred(xyRDD: RDD[TupleMatrixVector]): RDD[Array[Double]] = {
    val weights = this.weights
    xyRDD.flatMap {
      xy => {
        val x = xy.x
        val y = xy.y
        val iterator = new ListBuffer[Array[Double]]
        var i = 0
        while (i < y.length) {
          iterator += Array(y(i), ALinearModel.logisticPredictor(weights)(Vector(x.getRow(i))))
          i += 1
        }
        iterator
      }
    }
  }
}

/**
 * A base for LinearGradientLossFunctions supporting different XYDataTypes.
 * We don't provide the compute() hyper-function, because derived classes have to provide
 * the implementation that's unique to each XYDataType. For an example implementation, see
 * com.adatao.ML.LinearGradientLossFunction.
 */
abstract class ALinearGradientLossFunction[XYDataType](@transient XYData: XYDataType, ridgeLambda: Double)
  extends ALossFunction {

  final def computeLinearPredictor(X: Matrix, weights: Vector): DoubleMatrix = {
    X.mmul(weights)
  }

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
    numSamples = X.getRows()

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
      ALossFunction.safeLogOfSigmoid(Vector.fill(Y.length, 1.0).subi(hypothesis), linearPredictor.neg)) // (1-Y') x log(1-h)
    var J = -(lossA + lossB)
    if (ridgeLambda != 0.0) J += (ridgeLambda / 2) * weights.dot(weights)
    J
  }
}
