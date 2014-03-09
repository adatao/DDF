/**
 * 
 */
package com.adatao.ddf.content;


import java.io.Serializable;
import com.adatao.ddf.exception.DDFException;
import com.adatao.local.ddf.LocalDDF;


/**
 */
public interface ISerializable extends Serializable {
  void beforeSerialization() throws DDFException;

  void afterSerialization() throws DDFException;

  /**
   * Note this signature returns an ISerializable, which will be the ultimate object being returned from
   * deserialization. This means the object can take over at this point and return a different object instead. We use
   * this, for instance, as a trick in {@link LocalDDF}, to return the embedded object, instead of the container DDF
   * itself.
   * 
   * @param deserializedObject
   * @return
   * @throws DDFException
   */
  ISerializable afterDeserialization(ISerializable deserializedObject, Object serializationData) throws DDFException;
}
