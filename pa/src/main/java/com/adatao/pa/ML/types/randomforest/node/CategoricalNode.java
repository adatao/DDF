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
import com.adatao.ML.types.randomforest.node.Node;
import com.adatao.ML.types.randomforest.node.Node.Type;
import com.adatao.pa.ML.types.randomforest.data.Instance;

@SuppressWarnings("serial")
public class CategoricalNode extends Node {

	/** attribute index within an Instance */
	private int attr;

	/** possible values of this attribute */
	private double[] values;

	/** children of this Node */
	private Node[] childs;

	public CategoricalNode() {
	}

	public CategoricalNode(int attr, double[] values, Node[] children) {
		this.attr = attr;
		this.values = values.clone();
		this.childs = children.clone();
	}

	@Override
	public double classify(Instance instance) {
		//System.out.println(this.toString());
		int index = -1;
		for (int i = 0; i < values.length; i++) {
			if (instance.get(attr) == values[i]) {
				index = i;
				break;
			}
		}
		return (index != -1) ? childs[index].classify(instance) : Double.NaN;
	}

	@Override
	public Type getType() {
		return Type.CATEGORICAL;
	}

	@Override
	public String getString() {
		StringBuilder buffer = new StringBuilder();
		for (Node child : childs) {
			buffer.append(child).append(',');
		}
		buffer.append('.');
		return buffer.toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		attr = in.readInt();
		int length = in.readInt();
		values = new double[length];
		for (int i = 0; i < length; i++) {
			values[i] = in.readDouble();
		}
		length = in.readInt();
		childs = new Node[length];
		for (int i = 0; i < length; i++) {
			childs[i] = Node.read(in);
		}
	}

	@Override
	public void writeNode(DataOutput out) throws IOException {
		out.writeInt(attr);
		out.writeInt(values.length);
		for (double value : values) {
			out.writeDouble(value);
		}
		out.writeInt(childs.length);
		for (Node child : childs) {
			child.write(out);
		}
	}
}
