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

import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.pa.ML.types.randomforest.data.Data;
import com.adatao.pa.ML.types.randomforest.data.Instance;
import com.adatao.pa.ML.types.randomforest.node.CategoricalNode;
import com.adatao.pa.ML.types.randomforest.node.Leaf;
import com.adatao.pa.ML.types.randomforest.node.NumericalNode;
import com.adatao.pa.ML.types.randomforest.node.Node;

/**
 * Builds a classification tree or regression tree<br>
 * A classification tree is built when the target variable is a categorical
 * attribute.<br>
 * A regression tree is built when the target variable is a numerical attribute.
 */
public class TreeBuilder {
	private static final double EPSILON = 1.0e-6;
	private Data fullData;
	private Random random;
	
	private boolean[] selected; // attributes that have been selected when building a node
	private int nonSelected; // number of elements with value false in selected[] 
	private int m = 0; // number of attributes to select randomly at each node
	private int M;  // number of attributes in an instance, including predicted label

	private IgSplit igSplit; // algorithm to decide node splitting
	private boolean isRegression; // The problem is whether regression or not
	private double minVariance = 0.001; // minimum variance for split
	private int minSplitNum = 2; // minimum instances required to split

	public static Logger LOG = LoggerFactory.getLogger(TreeBuilder.class);

	public TreeBuilder(Data fullData_, long seed) {
		fullData = fullData_;
		random = new Random(seed); 
		isRegression = fullData.getDataFormat().isRegression();
		M = fullData.getDataFormat().numAttributes();
		if (isRegression) {
			m = (int) Math.ceil((M - 1) / 3.0);
			igSplit = new RegressionSplit();
		}
		else {
			m = (int) Math.ceil(Math.sqrt(M - 1));
			igSplit = new OptIgSplit();
		}
		selected = new boolean[M];
		selected[fullData.getDataFormat().getLabelId()] = true; // label is not selected
		nonSelected = M-1;
	}

	public void setMVars(int m) {
		this.m = m;
	}

	public void setMinVariance(double minVariance) {
		this.minVariance = minVariance;
	}

	public void setMinSplitNum(int minSplitNum) {
		this.minSplitNum = minSplitNum;
	}

	private int[] selectRandomAttributes() {
		int mm = Math.min(nonSelected, m);
		int[] res = new int[mm];
		
		boolean[] temp = new boolean[M];
		System.arraycopy(selected, 0, temp, 0, M);

		for (int i = 0; i < mm; i++) {
			int attr;
			do {
				attr = random.nextInt(M);
			} while (temp[attr]);
			temp[attr] = true;
			res[i] = attr;
		}
		return res;
	}
	
	public Node build() {
		return build(fullData);
	}
	
	public Node build(Data data) {
		if (data.isEmpty()) { 
			return new Leaf(Double.NaN);
		}

		double sumLabel = 0;
		for (int i = 0; i < data.size(); i++) {
			double label = data.get(i).get(data.getDataFormat().getLabelId());
			sumLabel += label;
		}
		
		int[] attributes = selectRandomAttributes();
				
		if (attributes.length == 0 || data.size() <= minSplitNum) {
			// we tried all the attributes and could not split the data anymore
			double label;
			if (isRegression)
				label = sumLabel / data.size();
			else
				label = data.majorityLabel(random);
			return new Leaf(label);
		}

		// termination conditions: 
		// 1. For regression: variance of labels is less than user-defined minVariance
		// 2. For classification: 
		// 2.1 all data instances are identical (ignoring selected attributes)
		// 2.2 all data instances have identical labels
		if (isRegression) {
			double sumSquared = 0;
			for (int i = 0; i < data.size(); i++) {
				double label = data.get(i).get(data.getDataFormat().getLabelId());
				sumSquared += label * label;
			}
			// variance = E(X^2) - E^2(X)
			double variance = (sumSquared - (sumLabel * sumLabel) / data.size()) / data.size();
			if (variance <= minVariance)
				return new Leaf(sumLabel / data.size());
		} else {
			if (allSameAttr(data)) {
				return new Leaf(data.majorityLabel(random));
			}
			if (data.allSameLabel()) {
				return new Leaf(data.get(0).get(data.getDataFormat().getLabelId()));
			}
		}

		// find the best split
		Split maxSplit = null;
		for (int attr : attributes) {
			Split split = igSplit.computeSplit(data, attr);			
			if (maxSplit == null || maxSplit.getIg() < split.getIg()) {
				maxSplit = split;
			}
		}

		// information gain or sum of squared error is near zero
		if (maxSplit.getIg() < EPSILON) {
			double label;
			if (isRegression)
				label = sumLabel / data.size();
			 else 
				label = data.majorityLabel(random);			
			return new Leaf(label);
		}
		
		if (data.getDataFormat().isNumerical(maxSplit.getAttr())) {
			// NUMERICAL: Split on attribute best.getAttr() at best.getSplit()
			Data[] subsets = data.subsetSplit(maxSplit.getAttr(), maxSplit.getSplit());
			Data lowSubset = subsets[0];
			Data highSubset = subsets[1];
			
			// if one of the subset is empty, we prevent further selection of this variable 
			if (lowSubset.isEmpty() || highSubset.isEmpty()) {
				selected[maxSplit.getAttr()] = true;
				nonSelected--;
			}
			Node lowChild = build(lowSubset);
			Node highChild = build(highSubset);
			
			if (lowSubset.isEmpty() || highSubset.isEmpty()) {
				selected[maxSplit.getAttr()] = false;
				nonSelected++;
			}
			return new NumericalNode(maxSplit.getAttr(), maxSplit.getSplit(), lowChild, highChild);
		} else {
			// CATEGORICAL: Split data on attribute best.getAttr()
			double[] values = data.values(maxSplit.getAttr());
			// divide data into branches at best.getAttr(). TODO: improve this by running once.
			Data subset;
			selected[maxSplit.getAttr()] = true;
			nonSelected--;
			Node[] children = new Node[values.length];
			for (int i = 0; i < values.length; i++) {
				subset = data.subsetEqual(maxSplit.getAttr(), values[i]);
				children[i] = build(subset);
			}
			selected[maxSplit.getAttr()] = false;
			nonSelected++;
			return new CategoricalNode(maxSplit.getAttr(), values, children);
		}
	}

	/***
	 * Check if all vectors have identical attribute values. Ignore selected
	 * attributes.
	 * 
	 * @param data
	 * @return true if all vectors are identical or the data is empty <br>
	 *         false otherwise
	 */
	private boolean allSameAttr(Data data) {
		if (data.isEmpty()) {
			return true;
		}
		Instance instance = data.get(0);
		for (int attr = 0; attr < selected.length; attr++)
			if (!selected[attr]) {
				for (int j = 1; j < data.size(); j++) {
					if (data.get(j).get(attr) != instance.get(attr)) {
						return false;
					}
				}
			}
		return true;
	}
}

