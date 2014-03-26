/*
 *  Copyright (C) 2013 Adatao, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.adatao.pa.spark.execution;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adatao.pa.AdataoException;
import com.adatao.pa.spark.DataManager;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.DataManager.DataContainer;
import com.adatao.pa.spark.DataManager.DataFrame;
import com.adatao.pa.spark.DataManager.MetaInfo;
import com.adatao.pa.spark.DataManager.SharkColumnVector;
import com.adatao.pa.spark.DataManager.SharkDataFrame;
import com.adatao.pa.spark.execution.Sql2DataFrame.Sql2DataFrameResult;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.FailResult;
import com.adatao.pa.spark.types.SuccessResult;
import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.adatao.ddf.DDF;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.content.Schema.Column;
import com.adatao.ddf.exception.DDFException;

@SuppressWarnings({ "serial", "unchecked", "rawtypes" })
public class Subset extends CExecutor {
	private String dataContainerID;
	private List<Column> columns;
	private Expr filter = null;
	DataManager dm;
	Broadcast<MetaInfo[]> broadcastMetaInfo;

	public static Logger LOG = LoggerFactory.getLogger(Subset.class);

	static public class ExprDeserializer implements JsonDeserializer<Expr> {
		public Expr deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
			throws JsonParseException {
			GsonBuilder gsonBld = new GsonBuilder();
			gsonBld.registerTypeAdapter(Expr.class, new ExprDeserializer());
			Gson gson = gsonBld.create();

			JsonObject jo = json.getAsJsonObject();
			String type = jo.get("type").getAsString();

			try {
				Expr expression = (Expr) gson.fromJson(json.toString(),
					Class.forName("com.adatao.pa.spark.execution.Subset$"+type));

				return expression;

			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				LOG.info(e.getMessage());
			}
			return null;
		}

	}

	// Base class for any expression node in the AST,
	// could be either an Operator or a Value
	static public class Expr implements Serializable {
		String type;
		public String getType() {
			return type;
		}
		public String toSql() {
			return null;
		}
		public void setType(String aType){
			type = aType;
        }
	}

	public enum OpName {lt, le, eq, ge, gt, ne, and, or, neg, isnull, isnotnull}

	// Base class for unary operations and binary operations
	static public class Operator extends Expr {
		OpName name;
		Expr[] operands;

		public OpName getName() {
			return name;
		}


		public Expr[] getOperands() {
			return operands;
		}

		@Override
		public String toString() {
			return "Operator [name=" + name + ", operands=" + Arrays.toString(operands) + "]";
		}

		@Override
		public String toSql() {
			if (name == null) {
				throw new IllegalArgumentException("missing Operator name from BigR client for operands[] "+Arrays.toString(operands));
			}
			switch (name) {
				case gt:
					return String.format("(%s > %s)", operands[0].toSql(), operands[1].toSql());
				case lt:
					return String.format("(%s < %s)", operands[0].toSql(), operands[1].toSql());
				case ge:
					return String.format("(%s >= %s)", operands[0].toSql(), operands[1].toSql());
				case le:
					return String.format("(%s <= %s)", operands[0].toSql(), operands[1].toSql());
				case eq:
					return String.format("(%s == %s)", operands[0].toSql(), operands[1].toSql());
				case ne:
					return String.format("(%s != %s)", operands[0].toSql(), operands[1].toSql());
				case and:
					return String.format("(%s AND %s)", operands[0].toSql(), operands[1].toSql());
				case or:
					return String.format("(%s OR %s)", operands[0].toSql(), operands[1].toSql());
				case neg:
					return String.format("(NOT %s)", operands[0].toSql());
				case isnull:
					return String.format("(%s IS NULL)", operands[0].toSql());
				case isnotnull:
					return String.format("(%s IS NOT NULL)", operands[0].toSql());
				default:
					throw new IllegalArgumentException("unsupported Operator: "+name);
			}
		}
	}

	public abstract static class Value extends Expr {
		public abstract Object getValue();
	}

	static public class IntVal extends Value {
		int value;
		@Override
		public String toString() {
			return "IntVal [value=" + value + "]";
		}
		@Override
		public Object getValue() {
			return value;
		}
		@Override
		public String toSql() {
			return Integer.toString(value);
		}
	}

	static public class DoubleVal extends Value {
		double value;
		@Override
		public String toString() {
			return "DoubleVal [value=" + value + "]";
		}
		@Override
		public Object getValue() {
			return value;
		}
		@Override
		public String toSql() {
			return Double.toString(value);
		}
	}

	static public class StringVal extends Value {
		String value;
		@Override
		public String toString() {
			return "StringVal [value=" + value + "]";
		}
		@Override
		public Object getValue() {
			return value;
		}
		@Override
		public String toSql() {
			return String.format("'%s'", value);
		}
	}

	static public class BooleanVal extends Value {
		Boolean value;
		@Override
		public String toString() {
			return "BooleanVal [value=" + value + "]";
		}
		@Override
		public Object getValue() {
			return value;
		}
		@Override
		public String toSql() {
			return Boolean.toString(value);
		}
	}

	static public class Column extends Expr {
		String id;
		String name;
		Integer index=null;

		public String getID() {
			return id;
		}

		public Integer getIndex() {
			return index;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setIndex(Integer index) {
			this.index = index;
		}

		public void setID(String id) {
			this.id = id;
		}

		public Object getValue(Object[] xs) {
			return xs[index];
		}

    @Override
    public String toSql() {
    	assert this.name != null;
			return this.name;
    }

		@Override
		public String toString() {
			return "Column [id=" + id + ", name=" + name + ", index=" + index
				+ "]";
		}

		public String getName() {
			return name;
		}
	}

	static public class FilterMapper extends Function<Object[], Boolean> {
		private static final long serialVersionUID = 3889875544995723350L;
		private Expr filter;
		public static Logger LOG = LoggerFactory.getLogger(FilterMapper.class);

		public FilterMapper(Expr filter) {
			this.filter = filter;
		}

		// evaluate a Column expression on a row objs,
		// or a literal Value expr
		private Object getValue(Expr expr, Object[] objs) {
			if (expr == null) {
				return null;
			}
			if (expr.getType().equals("Column")) {
					return ((Column) expr).getValue(objs);
			} else {
					return ((Value) expr).getValue();
			}
		}

		private Boolean eval(Expr expr, Object[] objs) {
			if (expr != null && expr.getType() != null && expr.getType().equals("Operator")) {
				Operator op = (Operator) expr;
				Expr[] operands = op.getOperands();
				Comparable<Object> lhs, rhs;
				switch (op.getName()) {
					case gt:
						lhs = (Comparable) getValue(operands[0], objs);
						rhs = (Comparable) getValue(operands[1], objs);
						return lhs.compareTo(rhs) > 0;
					case ge:
						lhs = (Comparable) getValue(operands[0], objs);
						rhs = (Comparable) getValue(operands[1], objs);
						return lhs.compareTo(rhs) >= 0;
					case lt:
						lhs = (Comparable) getValue(operands[0], objs);
						rhs = (Comparable) getValue(operands[1], objs);
						return lhs.compareTo(rhs) < 0;
					case le:
						lhs = (Comparable) getValue(operands[0], objs);
						rhs = (Comparable) getValue(operands[1], objs);
						return lhs.compareTo(rhs) <= 0;
					case eq:
						lhs = (Comparable) getValue(operands[0], objs);
						rhs = (Comparable) getValue(operands[1], objs);
						return lhs.compareTo(rhs) == 0;
					case ne:
						lhs = (Comparable) getValue(operands[0], objs);
						rhs = (Comparable) getValue(operands[1], objs);
						return lhs.compareTo(rhs) != 0;
					case and:
						return eval(operands[0], objs) && eval(operands[1], objs);
					case or:
						return eval(operands[0], objs) || eval(operands[1], objs);
					case neg:
						return !eval(operands[0], objs);
					case isnull:
						return getValue(operands[0], objs) == null;
					case isnotnull:
						return getValue(operands[0], objs) != null;
					default:
						throw new IllegalArgumentException("unsupported Operator: "+op.getName());
				}
			} else {
				throw new IllegalArgumentException("Subset eval() called with null Operator");
			}
		}

		@Override
		public Boolean call(Object[] objs) throws Exception {
			return eval(filter, objs);
		}

	}

	static public class ColumnSelectionMapper extends Function<Object[], Object[]> {
		private List<Column> columns;

		public ColumnSelectionMapper(List<Column> columns){
			this.columns = columns;
		}

		@Override
		public Object[] call(Object[] obj) throws Exception {
			int length = columns.size();
			Object[] retObj = new Object[length];
			for (int i=0; i<length; i++){
				retObj[i] = obj[columns.get(i).getIndex()];
			}
			return retObj;
		}

	}

	private MetaInfo[] selectColumnMetaInfo(DataContainer dc){
		int length = columns.size();
		MetaInfo[] retObj = new MetaInfo[length];
		for (int i=0; i<length; i++){
			retObj[i] = dc.getMetaInfo()[columns.get(i).getIndex()];
		}
		return retObj;
	}

	private void updateVectorIndex(Expr expr, DataContainer dc){
		if (expr == null) {
			return;
		}
		if (expr.getType().equals("Column")) {
			Column vec = (Column) expr;
			if (vec.getIndex() == null) {
				String name = vec.getName();
				if (name != null) {
					vec.setIndex(dc.getColumnIndexByName(name));
				}
			}
			return;
		}
		if (expr instanceof Operator) {
			Expr[] newOps = ((Operator) expr).getOperands();
			for (Expr newOp : newOps) {
				updateVectorIndex(newOp, dc);
			}
		}
	}

	private void updateVectorName(Expr expr, DataContainer dc){
		if (expr == null) {
			return;
		}
		if (expr.getType().equals("Column")) {
			Column vec = (Column) expr;
			if (vec.getName() == null) {
				Integer i = vec.getIndex();
				if (i != null) {
					vec.setName(dc.getColumnMetaInfoByIndex(i).getHeader());
				}
			}
			return;
		}
		if (expr instanceof Operator) {
			Expr[] newOps = ((Operator) expr).getOperands();
			for (Expr newOp : newOps) {
				updateVectorName(newOp, dc);
			}
		}
	}

	@Override
	public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
		// dm = sparkThread.getDataManager();
		// DataContainer dc = dm.get(dataContainerID);
		DDF ddf = (DDF) sparkThread.getDDFManager().getDDF(("SparkDDF-spark-" + dataContainerID).replace("-", "_"));
		
		if (columns == null || columns.size() != 1) {
		  return new FailResult().setMessage("Unsupport operation: only column project is supported.");
		}
		
		try {
  		// this is for the case of vector = one column
  		DDF vector = ddf.Views.project(columns.get(0).name);
  		sparkThread.getDDFManager().addDDF(vector);
  		String containerID = vector.getName().substring(15).replace("_", "-");
  		return new SubsetResult().setDataContainerID(containerID).setMetaInfo(generateMetaInfo(vector.getSchema()));
		} catch (DDFException e) {
		  return new FailResult().setMessage(e.getMessage());
		}
	}
	
	public static MetaInfo[] generateMetaInfo(Schema schema) {
    List<com.adatao.ddf.content.Schema.Column> columns = schema.getColumns();
    MetaInfo[] metaInfo = new MetaInfo[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      metaInfo[i] = new MetaInfo(columns.get(i).getName(), columns.get(i).getType().toString().toLowerCase());
    }
    return metaInfo;
  }

	@Override
	public String toString() {
		return "Subset [dataContainerID=" + dataContainerID + ", columns=" + columns
			+ ", filter=" + filter + "]";
	}

	static public class SubsetResult extends SuccessResult  {
		String dataContainerID;
		MetaInfo[] metaInfo;

		public SubsetResult setDataContainerID(String dataContainerID) {
			this.dataContainerID = dataContainerID;
			return this;
		}

		public String getDataContainerID() {
			return dataContainerID;
		}

		public SubsetResult setMetaInfo(MetaInfo[] metaInfo) {
			this.metaInfo = metaInfo.clone();
			return this;
		}

	}

	public String getDataContainerID() {
		return dataContainerID;
	}

	public Subset setDataContainerID(String dataContainerID) {
		this.dataContainerID = dataContainerID;
		return this;
	}

	public List<Column> getColumns() {
		return columns;
	}

	public Subset setColumns(List<Column> columns) {
		this.columns = columns;
		return this;
	}

	public Expr getFilter() {
		return filter;
	}

	public void setFilter(Expr filter) {
		this.filter = filter;
	}
}
