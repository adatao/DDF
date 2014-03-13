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

import org.scalatest.FunSuite

import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * Tests for cross validations
 *
 * @author aht
 *
 */
@RunWith(classOf[JUnitRunner])
class CrossValidationSuite extends FunSuite with SharedSparkContext {

	test("randomSplit") {
		val data = sc.parallelize(1 to 1000, 2)
		for (seed <- 1 to 5) {
			for (split <- CrossValidation.randomSplit(data, 5, 0.85, seed)) {
				val (train, test) = split
				val trainArray = train.collect
				val testSet = test.collect.toSet
				// We cannot be certain about the size of each set, which is ~ Bin(1000, 0.85)
				assertEquals(0.85, trainArray.size/1000.0, 0.025)
				assert(trainArray.forall(x => ! testSet.contains(x)), "train element found in test set!")
			}
		}
	}

	test("kFoldSplit") {
		val data = sc.parallelize(1 to 5000, 2)
		for (seed <- 1 to 3) {
			val betweenFolds = scala.collection.mutable.ArrayBuffer.empty[Set[Int]]
			for (fold <- CrossValidation.kFoldSplit(data, 5, seed)) {
				val (train, test) = fold
				val trainArray = train.collect
				val testSet = test.collect.toSet
				// We cannot be certain about the size of each set, since X ~ Bin(5000, 0.8)
				assertEquals(0.8, trainArray.size/5000.0, 0.02)
				assert(trainArray.forall(x => ! testSet.contains(x)), "train element found in test set!")
				betweenFolds += testSet
			}
			for (pair <- betweenFolds.toArray.combinations(2)) {
				val Array(a, b) = pair
				assert(a.intersect(b).isEmpty, "test set accross folds are not disjoint!")
			}
		}
	}
}
