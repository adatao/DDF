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

package com.adatao.pa.ML.types.randomforest.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import com.google.common.collect.Sets;

@SuppressWarnings("serial")
public class Data implements Serializable {

	private List<Instance> instances;
	private DataFormat dataFormat;

	public Data(DataFormat dataFormat, List<Instance> subset) {
		this.dataFormat = dataFormat;
		this.instances = subset;
	}

	public DataFormat getDataFormat() {
		return dataFormat;
	}

	public boolean isEmpty() {
		return instances.isEmpty();
	}

	public int size() {
		return instances.size();
	}

	public Instance get(int i) {
		return instances.get(i);
	}

	/***
	 * find the value of the major label, breaking ties randomly. This is done
	 * in CLASSIFICATION problem only.
	 * 
	 * @param rnd
	 * @return value of majority label
	 */
	public double majorityLabel(Random rnd) {
		// counts[i] = frequency of label value i
		int[] counts = new int[dataFormat.numLabels()];
		int labelId = dataFormat.getLabelId();
		for (int i = 0; i < size(); i++) {
			counts[(int) instances.get(i).get(labelId)]++;
		}

		int max = -1;
		List<Integer> maxId = new ArrayList<Integer>();
		for (int i = 0; i < dataFormat.numLabels(); i++) {
			if (max < counts[i]) {
				max = counts[i];
				maxId.clear();
				maxId.add(i);
			} else if (max == counts[i]) {
				maxId.add(i);
			}
		}
		int randomId = rnd.nextInt(maxId.size());
		return maxId.get(randomId);
	}

	/***
	 * @return true if all label are the same
	 */
	public boolean allSameLabel() {
		if (isEmpty()) {
			return true;
		}
		double label = instances.get(0).get(dataFormat.getLabelId());
		for (int i = 1; i < size(); i++) {
			if (instances.get(i).get(dataFormat.getLabelId()) != label) {
				return false;
			}
		}
		return true;
	}

	/***
	 * find all distinct original values of a given attribute
	 * 
	 * @param attr
	 * @return
	 */
	public double[] values(int attr) {
		Collection<Double> result = Sets.newHashSet();

		for (Instance instance : instances) {
			result.add(instance.get(attr));
		}

		double[] values = new double[result.size()];

		int index = 0;
		for (Double value : result) {
			values[index++] = value;
		}

		return values;
	}

	/***
	 * Split the data into two parts when we are considering a NUMERICAL Node.
	 * 
	 * @param attr
	 * @param split
	 * @return an array of 2 Data, the first one contains instances having
	 *         attribute attr less than the split value and the second one
	 *         contains the rest of the original Data.
	 */
	public Data[] subsetSplit(int attr, double split) {
		List<Instance> subsetLess = new ArrayList<Instance>();
		List<Instance> subsetNotLess = new ArrayList<Instance>();

		for (Instance instance : instances) {
			if (instance.get(attr) < split)
				subsetLess.add(instance);
			else
				subsetNotLess.add(instance);
		}
		Data[] dat = new Data[2];
		dat[0] = new Data(dataFormat, subsetLess);
		dat[1] = new Data(dataFormat, subsetNotLess);
		return dat;
	}

	/***
	 * Collect the instances of the same attribute when we are considering a
	 * CATEGORICAL Node
	 * 
	 * @param attr
	 * @param value
	 * @return the Data that contains instances whose attributes attr equal the
	 *         given value
	 */
	public Data subsetEqual(int attr, double value) {
		List<Instance> subset = new ArrayList<Instance>();
		for (Instance instance : instances) {
			if (instance.get(attr) == value) {
				subset.add(instance);
			}
		}
		return new Data(dataFormat, subset);
	}

	public String toString() {
		String res = "";
		for (Instance inst : instances)
			res += inst.toString() + '\n';
		return res;
	}


	public void sort() {
		Collections.sort(instances, new Comparator<Instance>(){

			@Override
			public int compare(Instance o1, Instance o2) {
				for (int i = 0; i < o1.size(); i++) {
					if (o1.get(i) < o2.get(i)) 
						return 1;
					else
						if (o1.get(i) > o2.get(i))
							return -1;
				}
				return 0;
			}});
	}
}
