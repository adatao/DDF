/**
 * 
 */
package com.adatao.ddf.content;


import com.adatao.ddf.exception.DDFException;


/**
 */
public interface IBeforeAndAfterSerDes {
  void beforeSerialization() throws DDFException;

  void afterDeserialization() throws DDFException;
}
