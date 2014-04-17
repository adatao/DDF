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

import com.adatao.ML.{ Utils ⇒ MLUtils, _ }
import com.adatao.ML.spark.{ Metrics, RddUtils }
import scala.Some
import com.adatao.ddf.DDF
import com.adatao.ddf.ml.IModel
import com.adatao.spark.ddf.SparkDDF
import com.adatao.pa.AdataoException
import com.adatao.pa.AdataoException.AdataoExceptionCode

/**
 *
 */
class R2Score(var dataContainerID: String, val xCols: Array[Int], val yCol: Int, var modelID: String) extends AExecutor[Double] {

  def runImpl(ctx: ExecutionContext): Double = {
    val ddfManager = ctx.sparkThread.getDDFManager();
    val ddf: DDF = ddfManager.getDDF(("SparkDDF-spark-" + dataContainerID).replace("-", "_"));

    // first, compute RDD[(ytrue, ypred)]
    //old API val predictions = getYtrueYpred(dataContainerID, modelID, xCols, yCol, ctx)

    val mymodel: IModel = ddfManager.getModel(modelID)
    if (mymodel == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_DATAFRAME_NONEXISTENT,
        String.format("Not found dataframe with id %s", modelID), null);
    }

    val predictionDDF = ddf.getMLSupporter().applyModel(mymodel, true, false)
    if (predictionDDF == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_DATAFRAME_NONEXISTENT,
        "Can not run prediction on dataContainerID: " + dataContainerID + "\t with modelID =" + modelID, null);
    }
    //get column mean
    val summary = ddf.getStatisticsSupporter().getSummary()
    val yMean = summary(yCol).mean()

    ddf.getMLMetricsSupporter().r2score(predictionDDF, yMean)

  }
}
