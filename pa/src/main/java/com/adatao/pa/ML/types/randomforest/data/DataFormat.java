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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.pa.spark.DataManager.MetaInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@SuppressWarnings("serial")
public class DataFormat implements Serializable {
	private final int[] xCols;
	private final int yCol; // label of the target attribute
	private final int labelId;
	private final boolean isRegression; // whether this problem is regression
	private final Attribute[] attributes; // type of attributes, length = xCols.length + 1 (including yCol)
	private final List<Map<String, Integer>> mapValues; // map values of CATEGORICAL attrs to index, null for NUMERICAL attr
	private List<Map<Integer, String>> reversedMapValues; // reversed map of mapValues

	public static Logger LOG = LoggerFactory.getLogger(DataFormat.class);

	private enum Attribute {
		NUMERICAL, CATEGORICAL;

		private boolean isNumerical() {
			return this == NUMERICAL;
		}

		private boolean isCategorical() {
			return this == CATEGORICAL;
		}
	}

	/**
	 * convert from metaInfo and xCols, yCol to DataFormat for TreeBuilder to
	 * process. This is done due to a historical reason. TreeBuilder and
	 * DataFormat were completed before integration of RandomForest to BigR
	 * 
	 * @param metaInfo
	 * @param xCols_
	 * @param yCol_
	 */
	public DataFormat(MetaInfo[] metaInfo, int[] xCols_, int yCol_) {
		this.xCols = xCols_.clone();
		this.yCol = yCol_;
		labelId = xCols.length;
		isRegression = !metaInfo[yCol].hasFactor();

		LOG.info(String.format("xCols = %s ", xCols));
		LOG.info(String.format("yCol  = %d ", yCol));
		LOG.info(String.format("This is a %s task", (isRegression) ? "regression" : "classification"));

		attributes = new Attribute[xCols.length + 1];
		mapValues = new ArrayList<Map<String, Integer>>();
		for (int i = 0; i < xCols.length; i++) {
			int col = xCols[i];
			if (metaInfo[col].hasFactor()) {
				attributes[i] = Attribute.CATEGORICAL;
				Object[] keySet = metaInfo[col].getFactor().keySet().toArray();
				int n = keySet.length;
				String[] names = new String[n];
				for (int j = 0; j < n; j++)
					names[j] = (String) keySet[j];
				Arrays.sort(names);
				Map<String, Integer> map = new HashMap<String, Integer>();
				for (int j = 0; j < n; j++) 
					map.put(names[j], j);
				mapValues.add(map);
			} else {
				attributes[i] = Attribute.NUMERICAL;
				mapValues.add(null);
			}
		}
		if (metaInfo[yCol].hasFactor()) {
			attributes[labelId] = Attribute.CATEGORICAL;
			Object[] keySet = metaInfo[yCol].getFactor().keySet().toArray();
			int n = keySet.length;
			String[] names = new String[n];
			for (int j = 0; j < n; j++)
				names[j] = (String) keySet[j];
			Arrays.sort(names);
			Map<String, Integer> map = new HashMap<String, Integer>();
			for (int j = 0; j < n; j++) 
				map.put(names[j], j);
			mapValues.add(map);
		} else {
			attributes[labelId] = Attribute.NUMERICAL;
			mapValues.add(null);
		}
		computeReversedMapValues();
	}

	public int numAttributes() {
		return attributes.length;
	}

	public int getLabelId() {
		return labelId;
	}

	public boolean isNumerical(int i) {
		return attributes[i].isNumerical();
	}

	public boolean isCategorical(int i) {
		return attributes[i].isCategorical();
	}

	public boolean isRegression() {
		return this.isRegression;
	}

	public List<Map<String, Integer>> getMapValues() {
		return mapValues;
	}

	public Map<String, Integer> getMapValues(int i) {
		return mapValues.get(i);
	}

	public int numLabels() {
		if (mapValues.get(labelId) == null) {
			LOG.error("Request number of label in a regression problem!" + " labelID = " + labelId);
			return -1;
		}
		return mapValues.get(labelId).size();
	}

	public int numValues(int i) {
		if (mapValues.get(i) == null) {
			LOG.error("Request number of values of numerical attribute: " + i);
			return -1;
		}
		return mapValues.get(i).size();
	}

	public double valueOf(int i, String token) {
		return mapValues.get(i).get(token);
	}

	// reverse a bijection: (string->index) becomes (index->string)
	public void computeReversedMapValues() {
		reversedMapValues = Lists.newArrayList();
		for (int i = 0; i < mapValues.size(); i++)
			if (mapValues.get(i) != null) {
				Map<Integer, String> rMap = Maps.newHashMap();
				Map<String, Integer> map = mapValues.get(i);
				for (String s : map.keySet()) {
					rMap.put(map.get(s), s);
				}
				reversedMapValues.add(rMap);
			} else {
				reversedMapValues.add(null);
			}
	}

	public Map<Integer, String> getRMapValues(int attr) {
		return this.reversedMapValues.get(attr);
	}

	public void printMapValues() {
		for (int i = 0; i < mapValues.size(); i++)
			if (mapValues.get(i) != null) {
				if (!isRegression && (i == mapValues.size() -1)) LOG.info("last map is of target");
				LOG.info("map value i= " + i);
				for (String s : mapValues.get(i).keySet()) {
					LOG.info(s + " " + mapValues.get(i).get(s));
				}
			}
	}

	public Instance convert(Object[] row) {
		double[] values = new double[xCols.length + 1];

		for (int i = 0; i < xCols.length; i++) {
			int col = xCols[i];
			if (mapValues.get(i) == null) {
				values[i] = Double.parseDouble(row[col].toString()); // NUMERICAL
			} else if (mapValues.get(i).containsKey(row[col].toString())) {
				values[i] = mapValues.get(i).get(row[col].toString()); // CATEGORICAL
			}
		}

		// the last element in the instance is the label
		if (mapValues.get(labelId) == null) {
			values[labelId] = Double.parseDouble(row[yCol].toString()); // NUMERICAL
		} else if (mapValues.get(labelId).containsKey(row[yCol].toString())) {
			values[labelId] = mapValues.get(labelId).get(row[yCol].toString()); // CATEGORICAL
		}

		return new Instance(values);
	}
}
