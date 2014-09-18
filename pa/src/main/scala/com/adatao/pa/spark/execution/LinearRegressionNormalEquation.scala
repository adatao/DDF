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
import com.adatao.spark.ddf.analytics.Utils
import java.util.HashMap
import io.ddf.ml.IModel
import io.ddf.DDF
import com.adatao.spark.ddf.etl.TransformationHandler._

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

        val transformedDDF = ddf.getTransformationHandler.dummyCoding(xColsName, yColName)
        
        val model = transformedDDF.ML.train("linearRegressionNQ", ridgeLambda: java.lang.Double)

        //TODO: get rid of this
        val rawModel = model.getRawModel.asInstanceOf[com.adatao.spark.ddf.analytics.NQLinearRegressionModel]
        if(transformedDDF.getSchema.getDummyCoding != null) {
          rawModel.setDummy(transformedDDF.getSchema.getDummyCoding)
        }

        model
      }
      case _ => throw new IllegalArgumentException("Only accept DDF")
    }
  }
}

