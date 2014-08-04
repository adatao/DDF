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

import com.adatao.spark.ddf.analytics.{ Utils ⇒ MLUtils, _ 
}
import scala.Some
import io.ddf.DDF
import io.ddf.ml.IModel
import io.spark.ddf.SparkDDF
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode
import com.adatao.spark.ddf.analytics.Utils
import io.ddf.ml.IModel
import io.ddf.DDF

/**
 *
 */
class R2Score(var dataContainerID: String, var modelID: String) extends AExecutor[Double] {

  def runImpl(ctx: ExecutionContext): Double = {
    val ddfManager = ctx.sparkThread.getDDFManager();
    val ddfId = Utils.dcID2DDFID(dataContainerID)
    val ddf: DDF = ddfManager.getDDF(ddfId);

    // first, compute RDD[(ytrue, ypred)]
    //old API val predictions = getYtrueYpred(dataContainerID, modelID, xCols, yCol, ctx)

    val model: IModel = ddfManager.getModel(modelID)
    if (model == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_DATAFRAME_NONEXISTENT,
        String.format("Not found dataframe with id %s", modelID), null);
    }
    val projectedDDF = ddf.VIEWS.project(model.getTrainedColumns: _*)

    val predictionDDF = projectedDDF.getMLSupporter().applyModel(model, true, false)
    if (predictionDDF == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_DATAFRAME_NONEXISTENT,
        "Can not run prediction on dataContainerID: " + dataContainerID + "\t with modelID =" + modelID, null);
    }
    //get column mean
    val summary = projectedDDF.getStatisticsSupporter().getSummary()
    val yMean = summary(projectedDDF.getNumColumns - 1).mean()

    ddf.getMLMetricsSupporter().r2score(predictionDDF, yMean)
  }
}
