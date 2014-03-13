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

package com.adatao.pa.ML.types.randomforest.node;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import com.adatao.pa.ML.types.randomforest.node.Node;
import com.adatao.pa.ML.types.randomforest.node.Node.Type;
import com.adatao.pa.ML.types.randomforest.data.Instance;

@SuppressWarnings("serial")
public class Leaf extends Node {

	/** value of the leaf node */
	private double label;

	public Leaf() {
	}

	public Leaf(double label) {
		this.label = label;
	}

	@Override
	public double classify(Instance instance) {
		//System.out.println(this.toString());
		return label;
	}

	@Override
	public Type getType() {
		return Type.LEAF;
	}

	@Override
	public String getString() {
		return Double.toString(label);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		label = in.readDouble();
	}

	@Override
	public void writeNode(DataOutput out) throws IOException {
		out.writeDouble(label);
	}
}
