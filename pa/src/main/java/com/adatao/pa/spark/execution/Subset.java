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


import java.lang.reflect.Type;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.ddf.DDF;
import com.adatao.ddf.content.ViewHandler.Column;
import com.adatao.ddf.content.ViewHandler.Expression;
import com.adatao.ddf.exception.DDFException;
import com.adatao.pa.AdataoException;
import com.adatao.pa.AdataoException.AdataoExceptionCode;
import com.adatao.pa.spark.DataManager.MetaInfo;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.Utils;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.SuccessResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;


@SuppressWarnings({ "serial", "unchecked", "rawtypes" })
public class Subset extends CExecutor {
  private String dataContainerID;
  private List<Column> columns;
  private Expression filter = null;

  public static Logger LOG = LoggerFactory.getLogger(Subset.class);


  static public class ExpressionDeserializer implements JsonDeserializer<Expression> {
    public Expression deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
        throws JsonParseException {
      GsonBuilder gsonBld = new GsonBuilder();
      gsonBld.registerTypeAdapter(Expression.class, new ExpressionDeserializer());
      Gson gson = gsonBld.create();

      JsonObject jo = json.getAsJsonObject();
      String type = jo.get("type").getAsString();

      try {
        Expression expression = (Expression) gson.fromJson(json.toString(),
            Class.forName("com.adatao.ddf.content.ViewHandler$" + type));

        return expression;

      } catch (ClassNotFoundException e) {
        // TODO Auto-generated catch block
        LOG.info(e.getMessage());
      }
      return null;
    }

  }


  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
    DDF ddf = sparkThread.getDDFManager().getDDF(dataContainerID);
    try {
      DDF subset = ddf.Views.subset(columns, filter);
      return new SubsetResult().setDataContainerID(subset.getName()).setMetaInfo(
          Utils.generateMetaInfo(subset.getSchema()));

    } catch (DDFException e) {
      throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, e.getMessage(), null);
    }
  }

  @Override
  public String toString() {
    return "Subset [dataContainerID=" + dataContainerID + ", columns=" + columns + ", filter=" + filter + "]";
  }


  static public class SubsetResult extends SuccessResult {
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

    public MetaInfo[] getMetaInfo(MetaInfo[] metaInfo) {
      return this.metaInfo;
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

  public Expression getFilter() {
    return filter;
  }

  public void setFilter(Expression filter) {
    this.filter = filter;
  }

}
