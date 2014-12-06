/*
 *  Copyright (C) 2013 Adatao, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.adatao.spark.ddf.analytics

import java.lang.String
import io.ddf.types.Vector
import io.ddf.types._
import org.apache.spark.rdd.RDD
import org.jblas.DoubleMatrix
import org.jblas.Solve
import org.jblas.exceptions.LapackArgumentException
import org.jblas.exceptions.LapackSingularityException
import org.jblas.exceptions.LapackException
import scala.collection.mutable.ArrayBuffer
import scala.Array.canBuildFrom
import io.ddf.content.Schema.DummyCoding
import io.ddf.exception.DDFException

/**
 * Author: NhanVLC
 * Original code is from mllib ridge regression
 */
object LinearRegressionNormalEquation {
  // Purpose of this function is to handle empty partitions using mapPartitions, :((((((
  def doMatrixCalculation(inputRows: TupleMatrixVector): TempCalculationValue = {
    // we should have 1 non-empty (Matrix, Vector) here
    val x = inputRows._1
    val y = inputRows._2
    
    val numFeatures: Int = x.getColumns()
    
    var XtX: DoubleMatrix = null
    var Xty: DoubleMatrix = null
    var nRows: Long = 0
    var y2: Double = 0
    var y1: Double = 0
    var x1: DoubleMatrix = null
    var numEmptyPartitions: Long = 0
  
    if (x.columns == 0 && y.columns == 0) {
      XtX = DoubleMatrix.zeros(numFeatures, numFeatures)
      Xty = DoubleMatrix.zeros(numFeatures, 1)
      x1 = DoubleMatrix.zeros(numFeatures, 1)
      numEmptyPartitions = 1
    } else {

      //tranpose y require significantly less memory than X because y is way smaller matrix

      // calculate X'X
      //        XtX = Xt.mmul(x)
      XtX = x.XtX

      // calculate X'y
      Xty = (y.transpose().mmul(x)).transpose()

      // for calculating feature variances var(x) = E(x^2) - E^2(x)
      // calculate sum(X)
      x1 = x.columnSums()

      // calculate number of rows
      nRows = x.getRows.toLong

      // those sum(y^2) and sum(y) are to calculate sum of squared total 
      // which is equal to population variance * number of rows
      // var(y) = E(y^2) - E^2(y)
      // sst(y) = var(y) * nRows
      // calculate sum(y^2)
      y2 = y.mul(y).sum()
      // calculate sum(y)
      y1 = y.sum()
    }

    if (nRows == 0) {
      XtX = DoubleMatrix.zeros(numFeatures, numFeatures)
      Xty = DoubleMatrix.zeros(numFeatures, 1)
      x1 = DoubleMatrix.zeros(numFeatures, 1)
      numEmptyPartitions = 1
    }

    new TempCalculationValue(XtX, Xty, nRows, y2, y1, x1, numEmptyPartitions)
  }

  def train(dataPartition1: RDD[TupleMatrixVector],  ridgeLambda: Double): NQLinearRegressionModel = {
    //Steps to solve Normal equation: w=(XtX)^-1 * Xty and coefficients' p-values
    //1. Compute XtX (Covariance matrix, Hessian matrix) , Xty distributedly.
    //2. Compute w and inverse of XtX in driver program.
    //3. Compute SSE (y-Xw)^2 distributedly.
    //4. Compute coefficients standard errors  sqrt(diag((XtX)-1)*SSE/(n-k-1)) in driver program.
    //5. Compute t-values and p-values in R based on coefficients’ standard errors
    // Ref: http://www.stat.purdue.edu/~jennings/stat514/stat512notes/topic3.pdf
    val ret = dataPartition1.map(xy => doMatrixCalculation(xy)).reduce((x, y) ⇒ (x.compute(y)))
    //val ret = dataPartition.filter(Xy ⇒ (Xy._1.columns > 0) && (Xy._2.rows > 0)).map(doMatrixCalculation).reduce((x, y) ⇒ (x._1.addi(y._1), x._2.addi(y._2), x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6.addi(y._6)))
    var messages: Array[String] = Array()

    if (ret.x3 == 0)
      throw new RuntimeException("No data to run, there is no rows, may be it is due to the null filtering process.")

    // sum of squared total
    val sst = ret.x4 - (ret.x5 * ret.x5) / ret.x3

    var XtXlambda: DoubleMatrix = ret.x1
    val numFeatures = XtXlambda.getColumns()

    if (ridgeLambda != 0) {
      XtXlambda = XtXlambda.addi(DoubleMatrix.eye(numFeatures).muli(ridgeLambda))
    }

    var w: DoubleMatrix = null
    var invXtX: DoubleMatrix = null
    var isPositive: Boolean = true
    var isSingular: Boolean = false
    try {
      w = Solve.solvePositive(XtXlambda, ret.x2)
      invXtX = Solve.solvePositive(XtXlambda, DoubleMatrix.eye(numFeatures))
    } catch {
      case lae: LapackArgumentException ⇒ {
        isPositive = false
        try {
          w = Solve.solveSymmetric(XtXlambda, ret.x2)
          invXtX = Solve.solveSymmetric(XtXlambda, DoubleMatrix.eye(numFeatures))
        } catch {
          case lse: LapackSingularityException ⇒ {
            isSingular = true
            try {
              w = Solve.solve(XtXlambda, ret.x2)
              invXtX = Solve.solve(XtXlambda, DoubleMatrix.eye(numFeatures))
            } catch {
              case le: LapackException ⇒ {
                throw new RuntimeException("The covariance matrix is singular. Please check independent variables for collinearity.\n" + XtXlambda.toString().replaceAll(";", "\n"))
              }
            }
          }
        }
      }
    }

    if (isSingular)
      messages :+= "The covariance matrix is singular. Please check independent variables for collinearity."

    // sum of deviation(x) = var(x) * numRows
    val sdX = ret.x1.diag().subi(ret.x6.muli(ret.x6).divi(ret.x3))

    // calculate VIF
    // Refs:
    // http://www3.nd.edu/~rwilliam/stats1/x91.pdf
    // https://sociology.byu.edu/Hoffmann/SiteAssets/Hoffmann%20_%20Linear%20Regression%20Analysis.pdf
    var vif = invXtX.diag().muli(sdX).toArray()
    vif = vif.takeRight(vif.length - 1) // remove the 1st element which corresponds to the intercept

    val residuals = dataPartition1.map {
      case (z) ⇒ {
        val x = z._1
        val y = z._2
        val yhat = x.mmul(w)
        val ymyhat = y.sub(yhat)
        ymyhat
      }
    }

    // Generate a DataFrame of residuals
    // Convert from Matrix to Array[Object]
    val residual_df = residuals.flatMap {
      m ⇒
        {
          var i = 0;
          val t = new ArrayBuffer[Array[Object]](m.length)
          while (i < m.length) {
            val arr = Array[Object](m.get(i).asInstanceOf[Object])
            t += arr
            i += 1
          }
          t
        }
    }
    /*
		val metaInfo = Array(new MetaInfo("residual", "java.lang.Double"))
		val res_df = new DataFrame(metaInfo, JavaRDD.fromRDD(residual_df))
		val sdf= SharkUtils.createSharkDataFrame(res_df, ctx.sparkThread.getSparkContext.asInstanceOf[JavaSharkContext])

		val sharkvector= SharkColumnVector.fromSharkDataFrame(sdf, metaInfo(0).getHeader)
		val res_df_id = ctx.sparkThread.getDataManager.add(sharkvector)
    */
    // residual sum of squares or sum of squared error
    val rss = residuals.map {
      res ⇒ res.muli(res).sum()
    }.reduce(_ + _)

    // degree of freedom
    val df = ret.x3 - numFeatures

    // standard errors
    val stderrs = org.jblas.MatrixFunctions.sqrt(invXtX.diag().muli(rss / df))
    // numFeatures - 1 -> we dont count intercept as a feature. Actually, the user can specify that he dont want the model to include intercept
    /*
        new NQLinearRegressionModel(Vector.apply(w), res_df_id, rss, sst, Vector.apply(stderrs), ret._3, numFeatures - 1, vif, messages)
        */
    new NQLinearRegressionModel(Vector.apply(w), rss, sst, Vector.apply(stderrs), ret.x3, numFeatures - 1, vif, messages)
  }

}

class NQLinearRegressionModel(weights: Vector, val rss: Double, val sst: Double, val stdErrs: Vector,
  numSamples: Long, val numFeatures: Int, val vif: Array[Double], val messages: Array[String]) extends AContinuousIterativeLinearModel(weights, null, numSamples) {

  override def predict(features: Vector): Double = this.linearPredictor(features)

  override def toString(): String = {
    val weightString = s"weights: [${weights.data.mkString(", ")}}]"
    val rssString = s"rss: ${rss}"
    val sstString = s"sst: $sst"
    val stdErrorsString = s"Standard errors: [${stdErrs.data.mkString(", ")}}]"
    val vifString = s"vif: [${vif.mkString(", ")}}]"
    this.getClass.getName + "\n" + weightString + "\n" + rssString + "\n" + stdErrorsString + "\n" + vifString
  }
}

class TempCalculationValue(var x1: DoubleMatrix, var x2: DoubleMatrix, var x3: Long, var x4: Double, var x5: Double, var x6: DoubleMatrix, var x7: Long) extends Serializable {

  def compute(other: TempCalculationValue): TempCalculationValue = {
    //    x._1.addi(y._1), x._2.addi(y._2), x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6.addi(y._6), x._7 + y._7
//    this.x1 = this.x1.addi(other.x1)
//    this.x2 = this.x2.addi(other.x2)
//    this.x3 = this.x3 + other.x3
//    this.x4 = this.x4 + other.x4
//    this.x5 = this.x5 + other.x5
//    this.x6 = this.x6.addi(other.x6)
//    this.x7 = this.x7 + other.x7

    var result: TempCalculationValue = new TempCalculationValue(this.x1.addi(other.x1), this.x2.addi(other.x2), this.x3 + other.x3, this.x4 + other.x4, this.x5 + other.x5, this.x6.addi(other.x6), this.x7 + other.x7)

    return (result)
  }
}
