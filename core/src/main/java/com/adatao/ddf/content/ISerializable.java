/**
 * 
 */
package com.adatao.ddf.content;


import java.io.Serializable;
import com.adatao.ddf.exception.DDFException;


/**
 */
public interface ISerializable extends Serializable {
  void beforeSerialization() throws DDFException;

  void afterDeserialization(Object data) throws DDFException;
}
