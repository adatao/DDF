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

package com.adatao.pa.spark.execution

import java.lang.String
import com.adatao.spark.ddf.analytics
import com.adatao.spark.ddf.analytics.ALossFunction
import com.adatao.spark.ddf.analytics.Utils
import io.ddf.types.Matrix
import io.ddf.types.Vector
import com.adatao.spark.ddf.analytics.RDDImplicits._
import org.apache.spark.rdd.RDD
import org.jblas.DoubleMatrix
import org.jblas.Solve
import com.adatao.pa.spark.DataManager._
import com.adatao.spark.ddf.analytics.ALinearModel
import java.util.HashMap
import scala.collection.mutable.ListBuffer
import org.jblas.exceptions.LapackArgumentException
import org.jblas.exceptions.LapackSingularityException
import org.jblas.exceptions.LapackException
import scala.collection.TraversableOnce
import scala.collection.Iterator
import scala.collection.immutable.List
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.api.java.JavaRDD
import java.util.ArrayList
import scala.collection.mutable.ArrayBuffer
import com.adatao.pa.spark.types.{FailedResult, ExecutionException, SuccessfulResult, ExecutionResult}
import com.adatao.spark.ddf.analytics.NQLinearRegressionModel
import io.ddf.ml.IModel
import io.ddf.DDF
import com.adatao.spark.ddf.etl.TransformationHandler

/**
 * Author: NhanVLC
 * Original code is from mllib ridge regression
 */
class LinearRegressionNormalEquation(
  dataContainerID: String,
  xCols: Array[Int],
  yCol: Int,
  var ridgeLambda: Double,
  mapReferenceLevel: HashMap[String, String] = null)
  extends AExecutor[IModel] {

  override def runImpl(context: ExecutionContext): IModel = {
    val ddfManager = context.sparkThread.getDDFManager();

    val ddfId = Utils.dcID2DDFID(dataContainerID)

    ddfManager.getDDF(ddfId) match {
      case ddf: DDF => {
        val xColsName = xCols.map{idx => ddf.getColumnName(idx)}
        val yColName = ddf.getColumnName(yCol)



        val transformedDDF = ddf.getTransformationHandler.asInstanceOf[TransformationHandler].dummyCoding(xColsName, yColName)
        //TODO fix this on opensource
        //TODO set factor for all string columns then compute factor levels
        transformedDDF.getSchemaHandler.computeFactorLevelsForAllStringColumns()
        transformedDDF.getSchemaHandler.generateDummyCoding()

        //TODO get rid of this
        val numFeatures: Int = if(transformedDDF.getSchema.getDummyCoding != null) {
          transformedDDF.getSchema.getDummyCoding.getNumberFeatures
        } else {
          xCols.length + 1
        }

        val model = transformedDDF.ML.train("linearRegressionNQ", numFeatures: java.lang.Integer, ridgeLambda: java.lang.Double)

        //TODO: get rid of this
        val rawModel = model.getRawModel.asInstanceOf[com.adatao.spark.ddf.analytics.NQLinearRegressionModel]
        if(transformedDDF.getSchema.getDummyCoding != null) {
          rawModel.setDummy(transformedDDF.getSchema.getDummyCoding)
        }

        model
      }
      case _ => throw new IllegalArgumentException("Only accept DDF")
    }
//    //project first
//    val trainedColumns = (xCols :+ yCol).map(idx => ddf.getColumnName(idx))
//    val projectedDDF = ddf.VIEWS.project(trainedColumns: _*)
//
//    projectedDDF.getSchemaHandler().computeFactorLevelsForAllStringColumns()
//    projectedDDF.getSchemaHandler().generateDummyCoding()
//
//    //plus bias term
//    var numFeatures = xCols.length + 1
//    if (projectedDDF.getSchema().getDummyCoding() != null) {
//      numFeatures = projectedDDF.getSchema().getDummyCoding().getNumberFeatures
//      projectedDDF.getSchema().getDummyCoding().toPrint()
//    }
//
//    val model = projectedDDF.ML.train("linearRegressionNQ", numFeatures: java.lang.Integer, ridgeLambda: java.lang.Double)
//
//    val rawModel = model.getRawModel.asInstanceOf[com.adatao.spark.ddf.analytics.NQLinearRegressionModel]
//    if (projectedDDF.getSchema().getDummyCoding() != null)
//      rawModel.setDummy(projectedDDF.getSchema().getDummyCoding())

  }
}

