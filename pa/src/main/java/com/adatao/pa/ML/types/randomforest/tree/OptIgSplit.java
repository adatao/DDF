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

package com.adatao.pa.ML.types.randomforest.tree;

import java.util.Arrays;
import com.adatao.pa.ML.types.randomforest.data.Data;
import com.adatao.pa.ML.types.randomforest.data.DataFormat;
import com.adatao.pa.ML.types.randomforest.data.DataUtils;
import com.adatao.pa.ML.types.randomforest.data.Instance;

/***
 * Optimized implementation of IgSplit<br>
 * This class can be used for classification problem.
 */
public class OptIgSplit extends IgSplit {

	private int[][] counts;
	private int[] countAll;
	private int[] countLess;

	@Override
	public Split computeSplit(Data data, int attr) {
		if (data.getDataFormat().isNumerical(attr)) {
			return numericalSplit(data, attr);
		} else {
			return categoricalSplit(data, attr);
		}
	}

	/**
	 * Instantiates the counting arrays
	 */
	private void initCounts(Data data, double[] values) {
		counts = new int[values.length][data.getDataFormat().numLabels()];
		countAll = new int[data.getDataFormat().numLabels()];
		countLess = new int[data.getDataFormat().numLabels()];
	}

	private void computeFrequencies(Data data, int attr, double[] values) {
		DataFormat dataFormat = data.getDataFormat();

		for (int index = 0; index < data.size(); index++) {
			Instance instance = data.get(index);
			counts[DataUtils.indexOf(values, instance.get(attr))][(int) instance.get(dataFormat.getLabelId())]++;
			countAll[(int) instance.get(dataFormat.getLabelId())]++;
		}
	}

	/**
	 * Computes the split for a CATEGORICAL attribute
	 */
	private Split categoricalSplit(Data data, int attr) {
		double[] values = data.values(attr);
		initCounts(data, values);
		computeFrequencies(data, attr, values);

		int size = data.size();
		double hy = entropy(countAll, size); // H(Y)
		double hyx = 0.0; // H(Y|X)
		double invDataSize = 1.0 / size;

		for (int index = 0; index < values.length; index++) {
			size = 0;
			for (int j = 0; j < counts[index].length; j++) {
				size += counts[index][j];
			}
			hyx += size * invDataSize * entropy(counts[index], size);
		}

		double ig = hy - hyx;
		return new Split(attr, ig);
	}

	/**
	 * Return the sorted list of distinct values for the given attribute
	 */
	private double[] sortedValues(Data data, int attr) {
		double[] values = data.values(attr);
		Arrays.sort(values);
		return values;
	}

	/**
	 * Computes the best split for a NUMERICAL attribute
	 */
	private Split numericalSplit(Data data, int attr) {
		double[] values = sortedValues(data, attr);

		initCounts(data, values);
		computeFrequencies(data, attr, values);

		int size = data.size();
		double hy = entropy(countAll, size);
		double invDataSize = 1.0 / size;

		int best = -1;
		double bestIg = -1.0;

		// try each possible split value
		for (int index = 0; index < values.length; index++) {
			double ig = hy;

			// instance with attribute value < values[index]
			size = DataUtils.sum(countLess);
			ig -= size * invDataSize * entropy(countLess, size);

			// instance with attribute value >= values[index]
			size = DataUtils.sum(countAll);
			ig -= size * invDataSize * entropy(countAll, size);

			if (ig > bestIg) {
				bestIg = ig;
				best = index;
			}

			DataUtils.add(countLess, counts[index]);
			DataUtils.dec(countAll, counts[index]);
		}

		if (best == -1) {
			throw new IllegalStateException("no best split found !");
		}
		return new Split(attr, bestIg, values[best]);
	}

	/**
	 * Computes the Entropy
	 * 
	 * @param counts
	 *            counts[i] = numInstances with label i
	 * @param dataSize
	 *            numInstances
	 */
	private static double entropy(int[] counts, int dataSize) {
		if (dataSize == 0) {
			return 0.0;
		}

		double entropy = 0.0;
		double invDataSize = 1.0 / dataSize;

		for (int count : counts) {
			if (count == 0) {
				continue; // otherwise we get a NaN
			}
			double p = count * invDataSize;
			entropy += -p * Math.log(p) / LOG2;
		}

		return entropy;
	}
}
