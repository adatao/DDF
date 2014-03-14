/**
 * Copyright 2013, ADATAO INC 
 * @author long@adatau.com
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.adatao.ML;

import java.io.Serializable;
import java.util.List;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.pa.ML.types.randomforest.node.Node;
import com.adatao.pa.ML.types.randomforest.data.DataFormat;
import com.adatao.pa.ML.types.randomforest.data.DataUtils;
import com.adatao.pa.ML.types.randomforest.data.Instance;
import com.google.common.base.Preconditions;

/***
 * Model of RandomForest which contains the list of predictive trees and one
 * DataFormat of the input
 * 
 * @author longpham@adatao.com
 * 
 */
@SuppressWarnings("serial")
public class RandomForestModel implements TModel, Serializable {
	private List<Node> trees; // list of decision trees
	@SuppressWarnings("unused")
  private Random random;
	private long seed;
	public DataFormat dataFormat;

	public static Logger LOG = LoggerFactory.getLogger(RandomForestModel.class);

	public RandomForestModel(DataFormat dataFormat, List<Node> trees, long seed) {
		this.dataFormat = dataFormat;
		this.trees = trees;
		this.seed = seed;
		this.random = new Random(seed);
	}
	
	public long getSeed() {
		return this.seed;
	}

	public int size() {
		return trees.size();
	}

	/**
	 * Predicts an instance: - In regression, return the predictive value. The
	 * closer the y truth, the better. - In classification, return the
	 * probability of class 0. So far, we only support metrics for binary
	 * classification
	 * 
	 * @return NaN if the label cannot be predicted
	 */
	public double predict(Instance instance) {
		if (dataFormat.isRegression()) { // REGRESSION
			int count = 0;
			double sum = 0;
			for (Node tree : trees) {
				double prediction = tree.classify(instance);
				if (!Double.isNaN(prediction)) {
					sum += prediction;
					count++;
				}
			}
			return (count > 0) ? (sum / count) : Double.NaN;
		} else { // CLASSIFICATION assume binary classification
			int[] sums = new int[dataFormat.numLabels()];
			for (Node tree : trees) {
				double prediction = tree.classify(instance);
				if (!Double.isNaN(prediction)) {
					Preconditions.checkArgument(0 <= prediction
							&& prediction < dataFormat.numLabels(),
							"prediction in CLASSIFICATION is out of range");
					sums[(int) prediction]++;
				}
			}
			// probability of class 1
			return (double) sums[1] / DataUtils.sum(sums);

			// index of the most probable class
			// return (DataUtils.sum(sums) > 0) ? DataUtils.maxIndex(random,
			// sums) : Double.NaN;
		}
	}

	/**
	 * Predicts the label from a raw Object[]. Convert raw Object[] to Instance
	 * then perform predict(Instance).
	 * 
	 * @return NaN if the label cannot be predicted
	 */
	public double predict(Object[] row) {
		return predict(dataFormat.convert(row));
	}

	@Override
	public String toString() {
		return String.format("RandomForestModel[isRegression=%s size=%d seed=%d]",
				dataFormat.isRegression(), size(), this.seed);
	}

	@Override
	public Logger _LOG() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void _LOG_$eq(Logger x$1) {
		// TODO Auto-generated method stub

	}

	@Override
	public Logger LOG() {
		// TODO Auto-generated method stub
		return null;
	}
}
