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
import scala.collection.JavaConversions._
import com.adatao.spark.ddf.analytics.Utils

//  @author aht

/*
* Return an Array of Tuple (train, test) of dataContainerID
* */
class CVRandomSplit(dataContainerID: String, numSplits: Int, trainingSize: Double, seed: Long) extends AExecutor[Array[Array[String]]] {
  override def runImpl(ctx: ExecutionContext): Array[Array[String]] = {

    val ddfId = Utils.dcID2DDFID(dataContainerID)
    val ddf = ctx.sparkThread.getDDFManager().getDDF(ddfId);
    val cvSets = ddf.ML.CVRandom(numSplits, trainingSize, seed)
    val result = cvSets.map {
      set =>
        {
          val train = set(0).getName
          val test = set(1).getName
          Array(train, test)
        }
    }
    result.toArray
  }
}

