package com.adatao.ddf.ml;


import java.io.Serializable;
import java.lang.reflect.Type;
import java.lang.reflect.Modifier;
import java.util.*;

import com.adatao.basic.ddf.BasicDDF;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.types.TJsonSerializable;
import com.adatao.ddf.types.TJsonSerializable$class;
import com.google.gson.*;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.ml.MLClassMethods.PredictMethod;
import org.apache.commons.lang.StringUtils;

/**
 */

public class Model implements IModel, Serializable {

  private static final long serialVersionUID = 8076703024981092021L;

  private Object mRawModel;

  private String mRawModelClass;

  private String mName;

  private String mClass = this.getClass().getName(); //for serialization

  public Model(Object rawModel) {
    mRawModel = rawModel;

    if(rawModel != null) {
      mRawModelClass = mRawModel.getClass().getName();
    } else {
      mRawModelClass = null;
    }

    mName = UUID.randomUUID().toString();
  }

  @Override
  public Object getRawModel() {
    return mRawModel;
  }

  @Override
  public void setRawModel(Object rawModel) {
    this.mRawModel = rawModel;
    this.mRawModelClass = rawModel.getClass().getName();
  }

  @Override
  public String getName() {
    return mName;
  }

  @Override
  public void setName(String name) {
    mName = name;
  }

  @Override
  public Double predict(double[] point) throws DDFException {

    PredictMethod predictMethod = new PredictMethod(this.getRawModel(), MLClassMethods.DEFAULT_PREDICT_METHOD_NAME,
        new Class<?>[] { point.getClass() });

    if (predictMethod.getMethod() == null) {
      throw new DDFException(String.format("Cannot locate method specified by %s",
          MLClassMethods.DEFAULT_PREDICT_METHOD_NAME));
    }

    Object prediction = predictMethod.instanceInvoke(point);

    if (prediction instanceof Double) {
      return (Double) prediction;

    } else if (prediction instanceof Integer) {
      return ((Integer) prediction).doubleValue();

    } else {
      throw new DDFException(String.format("Error getting prediction from model %s", this.getRawModel().getClass()
          .getName()));
    }

  }

  @Override
  public String toString() {

    Gson gson = new Gson();
    return gson.toJson(this.mRawModel);
  }

  @Override
  public String toJson() {
    Gson gson = new Gson();
    return gson.toJson(this);
  }

  public static Model fromJson(String json) throws DDFException {
    Gson gson = new GsonBuilder().registerTypeAdapter(Model.class, new ModelDeserializer()).create();

    Model deserializedModel = gson.fromJson(json, Model.class);

    if(deserializedModel == null) {
      throw new DDFException(String.format("Error deserialize json: %s", json));

    } else {
      return deserializedModel;
    }
  }

  @Override
  public DDF serialize2DDF(DDFManager manager) throws DDFException {
    String json = this.toJson();

    Gson gson = new Gson();
    Map<String, Object> keyValueMap = gson.fromJson(json, Map.class);
    JsonObject jsonObject = (new JsonParser().parse(json)).getAsJsonObject();

    Set<String> keyset = keyValueMap.keySet();
    Iterator<String> keys = keyset.iterator();
    List<String> listColumns = new ArrayList<String>();
    List<String> listValues = new ArrayList<String>();

    while(keys.hasNext()) {
      String key = keys.next();
      String value = jsonObject.get(key).toString();
      listColumns.add(String.format(("%s string"),key));
      listValues.add(value);
    }

    // Create schema for DDF
    String columns = StringUtils.join(listColumns, ", ");

    Schema schema = new Schema(null, columns);

    return new BasicDDF(manager, listValues, String.class, manager.getNamespace(), null, schema);
  }

  public static Model deserializeFromDDF(BasicDDF ddf) throws DDFException {
    List<String> data = ddf.getList(String.class);
    List<String> metaData= new ArrayList<String>();
    List<Schema.Column> columns = ddf.getSchema().getColumns();

    for(Schema.Column col: columns) {
      metaData.add(col.getName());
    }
    if(metaData.size() != data.size()) {
      throw new DDFException("");
    }

    List<String> listJson = new ArrayList<String>();

    for(int i = 0; i < metaData.size(); i++) {
      String key = metaData.get(i);
      String value = data.get(i);
      listJson.add(String.format("\"%s\":%s",key, value));
    }

    String json = StringUtils.join(listJson, ",");
    return Model.fromJson(String.format("{%s}", json));
  }

  static class ModelDeserializer implements JsonDeserializer<Model> {
    private Gson _gson = new Gson();

    @Override
    public Model deserialize(JsonElement jElement, Type theType, JsonDeserializationContext context) {

      if (jElement instanceof JsonObject)  {
        JsonObject jsonObj = (JsonObject) jElement;
        try {
          String clazz = jsonObj.get("mClass").getAsString();

          if(!clazz.equals(Model.class.getName())) {
            return null;
          }

          Class<?> rawModelClass = Class.forName(jsonObj.get("mRawModelClass").getAsString());
          Object rawModel = _gson.fromJson(jsonObj.get("mRawModel"), rawModelClass);
          jsonObj.remove("mRawModel");

          Model deserializedModel = _gson.fromJson(jsonObj, Model.class);
          deserializedModel.setRawModel(rawModel);

          return deserializedModel;
        } catch (Exception e) {
          return null;
        }
      } else {
        return null;
      }
    }
  }
}
