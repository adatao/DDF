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

package com.adatao.ML.spark

// @author: aht

import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.rdd.RDD
import java.util.Random

private[adatao]
class SeededPartition(val prev: Partition, val seed: Int) extends Partition with Serializable {
  override val index: Int = prev.index
}

private[adatao]
class RandomSplitRDD[T: ClassManifest](
    prev: RDD[T],
    seed: Long,
    lower: Double,
    upper: Double,
    isTraining: Boolean)
  extends RDD[T](prev) {

  override def getPartitions: Array[Partition] = {
    val rg = new Random(seed)
    firstParent[T].partitions.map(x => new SeededPartition(x, rg.nextInt))
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[T].preferredLocations(split.asInstanceOf[SeededPartition].prev)

  override def compute(splitIn: Partition, context: TaskContext): Iterator[T] = {
    val split = splitIn.asInstanceOf[SeededPartition]
    val rand = new Random(split.seed)
    // This is equivalent to taking a random sample without replacement
    // for for the train set and leave the rest as the test set.
    // The results are stable accross compute() calls due to deterministic RNG.
    if (isTraining) {
	    firstParent[T].iterator(split.prev, context).filter(x => {val z = rand.nextDouble; z < lower || z >= upper})
	  } else {
	  	firstParent[T].iterator(split.prev, context).filter(x => {val z = rand.nextDouble; lower <= z && z < upper})
	  }
  }
}

object CrossValidation {
	/** Return an Iterator of size k of (train, test) RDD Tuple
	  * for which the probability of each element belonging to each split is (trainingSize, 1-trainingSize).
	  * The train & test data across k split are shuffled differently (different random seed for each iteration).
	  */
	def randomSplit[T](rdd: RDD[T], numSplits: Int, trainingSize: Double, seed: Long)(implicit _cm: ClassManifest[T]): Iterator[(RDD[T], RDD[T])] = {
		require(0 < trainingSize && trainingSize < 1)
		val rg = new Random(seed)
		(1 to numSplits).map(_ => rg.nextInt).map(z =>
			(new RandomSplitRDD(rdd, z, 0, 1.0-trainingSize, true),
				new RandomSplitRDD(rdd, z, 0, 1.0-trainingSize, false))).toIterator
	}

	/** Return an Iterator of of size k of (train, test) RDD Tuple 
	  * for which the probability of each element belonging to either split is ((k-1)/k, 1/k).
	  * The location of the test data is shifted consistently between folds
	  * so that the resulting test sets are pair-wise disjoint.
	  */
	def kFoldSplit[T](rdd: RDD[T], numSplits: Int, seed: Long)(implicit _cm: ClassManifest[T]): Iterator[(RDD[T], RDD[T])] = {
		require(numSplits > 0)
		(for (lower <- 0.0 until 1.0 by 1.0/numSplits)
			yield (new RandomSplitRDD(rdd, seed, lower, lower+1.0/numSplits, true),
				new RandomSplitRDD(rdd, seed, lower, lower+1.0/numSplits, false))
		).toIterator
	}
}
