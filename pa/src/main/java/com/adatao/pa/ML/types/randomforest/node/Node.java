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
import java.io.Serializable;
import org.apache.hadoop.io.Writable;
import com.adatao.pa.ML.types.randomforest.data.Instance;
import com.adatao.pa.ML.types.randomforest.node.CategoricalNode;
import com.adatao.pa.ML.types.randomforest.node.Leaf;
import com.adatao.pa.ML.types.randomforest.node.NumericalNode;

@SuppressWarnings("serial")
public abstract class Node implements Serializable, Writable {

  public enum Type {
    LEAF, NUMERICAL, CATEGORICAL;
  }

  public abstract double classify(Instance instance);

  public abstract Type getType();

  public static Node read(DataInput in) throws IOException {
    Type type = Type.values()[in.readInt()];
    Node node;

    switch (type) {
    case LEAF:
      node = new Leaf();
      break;
    case NUMERICAL:
      node = new NumericalNode();
      break;
    case CATEGORICAL:
      node = new CategoricalNode();
      break;
    default:
      throw new IllegalStateException("This implementation is not currently supported");
    }
    node.readFields(in);
    return node;
  }

  @Override
  public final String toString() {
    return getType() + ":" + getString();
  }

  public abstract String getString();

  @Override
  public final void write(DataOutput out) throws IOException {
    out.writeInt(getType().ordinal());
    writeNode(out);
  }

  public abstract void writeNode(DataOutput out) throws IOException;
}
