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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import com.adatao.pa.ML.types.randomforest.data.Data;
import com.adatao.pa.ML.types.randomforest.data.Instance;

/***
 * Regression problem implementation of IgSplit. <br>
 * This class can be used for regression problem.
 */
public class RegressionSplit extends IgSplit {

	@SuppressWarnings("serial")
	private static class InstanceComparator implements Comparator<Instance>, Serializable {
		private final int attr;

		InstanceComparator(int attr) {
			this.attr = attr;
		}

		@Override
		public int compare(Instance ins0, Instance ins1) {
			return Double.compare(ins0.get(attr), ins1.get(attr));
		}
	}

	@Override
	public Split computeSplit(Data data, int attr) {
		if (data.getDataFormat().isNumerical(attr)) {
			return numericalSplit(data, attr);
		} else {
			return categoricalSplit(data, attr);
		}
	}
	
	private Split categoricalSplit(Data data, int attr) {
		int labelId = data.getDataFormat().getLabelId();

		// Var(X) = E^2(X) - E(X^2)
		double[] sum = new double[data.getDataFormat().numValues(attr)];
		double[] sum2 = new double[data.getDataFormat().numValues(attr)];
		int[] count = new int[data.getDataFormat().numValues(attr)];
		
		double S = 0, S2 = 0;
		for (int i = 0; i < data.size(); i++) {
			Instance instance = data.get(i);
			double y = instance.get(labelId);
			int x = (int) instance.get(attr);
			sum[x] += y;
			sum2[x] += y*y;
			count[x]++;
			S += y;
			S2 += y*y;
		}
		
		double var = 0;
		for (int x = 0; x < data.getDataFormat().numValues(attr); x++) {
			var += sum2[x] - (sum[x]*sum[x])/count[x];
		}
		// target = var(total) - sum(var(y)) for each subset y that x belongs to one category. 
		return new Split(attr, S2 - S*S/data.size() - var);
	}


	/***
	 * computes the best split for a NUMERICAL attribute
	 * 
	 * @param data
	 * @param attr
	 * @return
	 */	
	private Split numericalSplit(Data data, int attr) {		
		int labelId = data.getDataFormat().getLabelId();
		// Instance sort
		Instance[] instances = new Instance[data.size()];
		for (int i = 0; i < data.size(); i++) {
			instances[i] = data.get(i);
		}
		Arrays.sort(instances, new InstanceComparator(attr));
		
		// compute sum of y and sum of y^2, for efficient running time
		double S = 0, S2 = 0;
		for (int i = 0; i < instances.length; i++) {
			double y = instances[i].get(labelId); 
			S += y;
			S2 += y*y;
		}
		
		int countLeft = 0, countRight = instances.length; 
		double sum = 0, sum2 = 0, split = Double.NaN, min = Double.MAX_VALUE;
		// At each split, calculate the sum of variance of two halves.
		// TODO should we try a split so that one of the set is empty ?
		for (int i = 0; i < instances.length-1; i++) {			
			countLeft++;
			countRight--;
			double y = instances[i].get(labelId); 
			sum += y;
			sum2 += y*y;
			double var = sum2 			- Math.pow(sum,2) /	countLeft + 
						 (S2 - sum2)	- Math.pow(S-sum, 2) / countRight;
			if (min > var) {
				min = var;
				split = (instances[i].get(attr) + instances[i+1].get(attr)) / 2;
			}
		}
		// target = var(total) - min(var(y-) + var(y+)) where y- and y+ are two subset of one split.
		return new Split(attr, (S2 - (S*S)/instances.length)-min, split);
	}

	
	/**
	 * TODO: This should give the same value as numericalSplit but it doesn't and I don't know why. Can someone have a look ?
	 * 
	 * @param data
	 * @param attr
	 * @return
	 */
	@SuppressWarnings("unused")
  private Split numericalSplit1(Data data, int attr) {		
		int labelId = data.getDataFormat().getLabelId();
		// Instance sort
		Instance[] instances = new Instance[data.size()];
		for (int i = 0; i < data.size(); i++) {
			instances[i] = data.get(i);
		}
		Arrays.sort(instances, new InstanceComparator(attr));
		
		// compute sum of y and sum of y^2, for efficient running time
		double S = 0, S2 = 0;
		for (int i = 0; i < instances.length; i++) {
			double y = instances[i].get(labelId); 
			S += y;
			S2 += y*y;
		}
		
		int countLeft = 0, countRight = instances.length; 
		double sum = 0, sum2 = 0, split = Double.NaN, min = Double.MAX_VALUE;
		// At each split, calculate the sum of variance of two halves.
		// TODO should we try a split so that one of the set is empty ?
		for (int i = 0; i < instances.length-1; i++) {			
			countLeft++;
			countRight--;
			double y = instances[i].get(labelId); 
			sum += y;
			sum2 += y*y;
			double var = sum2 			- Math.pow(sum,2) /	countLeft + 
						 (S2 - sum2)	- Math.pow(S-sum, 2) / countRight;
			if (min > var) {
				min = var;
				split = (instances[i].get(attr) + instances[i+1].get(attr)) / 2;
			}
		}
		// target = var(total) - min(var(y-) + var(y+)) where y- and y+ are two subset of one split.
		return new Split(attr, -min, split);
	}

}
