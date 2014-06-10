package com.adatao.ddf.ml;


import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

import com.adatao.ddf.types.TJsonSerializable;
import com.adatao.ddf.types.TJsonSerializable$class;
import com.google.gson.Gson;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.ml.MLClassMethods.PredictMethod;

/**
 */

public class Model implements IModel, Serializable, TJsonSerializable {

  private static final long serialVersionUID = 8076703024981092021L;

  private Object mRawModel;

  private String mName;

  private String mclazz;

  public Model(Object rawModel) {
    mRawModel = rawModel;
    mName = UUID.randomUUID().toString();
  }

 @Override
 public TJsonSerializable fromJson(String json) {
    return TJsonSerializable$class.fromJson(this, json);
 }

  @Override
  public String toJson() {
    return TJsonSerializable$class.toJson(this);
  }

  @Override
  public void com$adatao$ddf$types$TJsonSerializable$_setter_$clazz_$eq(String Aclass) {
    mclazz = Aclass;
  }

  public String clazz() {
    return mclazz;
  }

  @Override
  public Object getRawModel() {
    return mRawModel;
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
//    Map<String, String> attributes = new HashMap<String, String>();
//    Object source = this.getRawModel();
//
//    if(! source.getClass().isArray()) {
//      try {
//        Field[] fields = getInstanceVariables(source.getClass());
//        System.out.println(">>>>>>>> fields.length = " + fields.length);
//        for (int i = 0; i < fields.length; i++) {
//          if (!Modifier.isPublic(fields[i].getModifiers())) {
//            fields[i].setAccessible(true);
//          }
//          String name = fields[i].getName();
//          Class fieldType = fields[i].getType();
//          Object attribute = fields[i].get(source);
//          String strValue = serializeVariable(fieldType, attribute);
//
//          attributes.put(name, strValue);
//        }
//        String returnValue = "";
//        for(String name: attributes.keySet()) {
//          String value = attributes.get(name);
//          returnValue = returnValue.concat(name + ": " + value);
//        }
//        return returnValue;
//      } catch (Exception e) {
//        return e.toString();
//      }
//    } else {
//      return "asdasd12@!#!@as";
//    }
    return this.toJson();
  }

//  private String serializeVariable(Class fieldType, Object attribute) throws DDFException {
//    if(attribute == null) {
//      return "null";
//    }
//    else {
//      return attribute.toString();
//    }
//  }
//
//  private Field[] getInstanceVariables(Class cls) {
//    List accum = new LinkedList();
//    while(cls != null) {
//      Field[] fields = cls.getDeclaredFields();
//      for(int i = 0; i< fields.length; i++) {
//        if(!Modifier.isStatic(fields[i].getModifiers())) {
//          accum.add(fields[i]);
//        }
//      }
//      cls = cls.getSuperclass();
//    }
//    Field[] returnValue = new Field[accum.size()];
//    return (Field[]) accum.toArray(returnValue);
//  }
}
