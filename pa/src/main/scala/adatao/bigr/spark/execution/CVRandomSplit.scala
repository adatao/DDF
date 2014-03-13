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

package adatao.bigr.spark.execution

import adatao.ML.spark.CrossValidation
import adatao.bigr.spark.DataManager.DataFrame
import adatao.bigr.spark.SharkUtils
import shark.api.JavaSharkContext

//  @author aht

/*
* Return an Array of Tuple (train, test) of dataContainerID
* */
class CVRandomSplit(dataContainerID: String, numIter: Int, trainingSize: Double, seed: Long) extends AExecutor[Array[Array[String]]] {
	override def runImpl(ctx: ExecutionContext): Array[Array[String]] = {
		getRDD(dataContainerID, ctx) match {
			case Some(rdd) => {
				val splits = CrossValidation.randomSplit(rdd, numIter, trainingSize, seed)
				val dm = ctx.sparkThread.getDataManager
				val metaInfo = dm.get(dataContainerID).getMetaInfo
				val result = splits.map(split => {
					val (train, test) = split
					val jsc = ctx.sparkThread.getSparkContext.asInstanceOf[JavaSharkContext]
					val traindf = SharkUtils.createSharkDataFrame(new DataFrame(metaInfo, train), jsc)
					val testdf = SharkUtils.createSharkDataFrame(new DataFrame(metaInfo, test), jsc)
					// add to DataManager, returning UID
					Array(dm.add(traindf), dm.add(testdf))
				}).toArray
				LOG.info("CVRandomSplit result = {}", result)
				result
			}
			case _ => throw new IllegalArgumentException("dataContainerID not found")
		}
	}
}

