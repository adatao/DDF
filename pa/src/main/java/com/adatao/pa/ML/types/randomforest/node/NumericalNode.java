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
import com.adatao.pa.ML.types.randomforest.data.Instance;

@SuppressWarnings("serial")
public class NumericalNode extends Node {

	/** numerical attribute to split for */
	private int attr;

	/** split value */
	private double split;

	/** child node when attribute's value < split value */
	private Node loChild;

	/** child node when attribute's value >= split value */
	private Node hiChild;

	public NumericalNode() {
	}

	public NumericalNode(int attr, double split, Node loChild, Node hiChild) {
		this.attr = attr;
		this.split = split;
		this.loChild = loChild;
		this.hiChild = hiChild;
	}

	@Override
	public double classify(Instance instance) {
		return (instance.get(attr) < split) ? loChild.classify(instance) : hiChild.classify(instance);
	}

	@Override
	public Type getType() {
		return Type.NUMERICAL;
	}

	@Override
	public String getString() {
		return "@" + split + " " + loChild.toString() + ',' + hiChild.toString() + '.';
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		attr = in.readInt();
		split = in.readDouble();
		loChild = Node.read(in);
		hiChild = Node.read(in);
	}

	@Override
	public void writeNode(DataOutput out) throws IOException {
		out.writeInt(attr);
		out.writeDouble(split);
		loChild.write(out);
		hiChild.write(out);
	}
}
