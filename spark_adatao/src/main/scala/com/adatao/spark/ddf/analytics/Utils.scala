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

import java.util.HashMap
import java.util.HashMap

import scala.collection.mutable.ListBuffer
import scala.util.Random

import io.ddf.types.Matrix
import io.ddf.types.Vector
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import java.util

object Utils {

  // random initial weights (uniform distributed)
  def randWeights(numFeatures: Int) = Vector(Seq.fill(numFeatures)(Random.nextDouble).toArray)

}

