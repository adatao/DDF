/**
 * 
 */
package com.adatao.ddf.content;

import java.util.Iterator;

import com.adatao.ddf.DDF;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;

/**
 *
 */
public interface IHandleViews extends IHandleDDFFunctionalGroup {

  /**
   * Gets an iterator that walks through all rows in this DDF, each time returning a row element of
   * type rowType
   * 
   * @param rowType
   * @return
   */
  public <T> Iterator<T> getRowIterator(Class<T> rowType);

  /**
   * Gets an iterator that walks through all rows in this DDF, each time returning a row element of
   * the default rowType
   * 
   * @return
   */
  public Iterator<?> getRowIterator();

  /**
   * Gets an iterator that walks through all rows from a particular column in this DDF, each time
   * returning a single element from that column.
   * 
   * @param rowType
   * @param columnType
   * @param columnIndex
   * @return
   */
  public <R, C> Iterator<C> getElementIterator(Class<R> rowType, Class<C> columnType, int columnIndex);


  /**
   * Gets an iterator that walks through all rows from a particular column in this DDF, each time
   * returning a single element from that column. Use default for the rowType & columnType.
   * 
   * @param columnIndex
   * @return
   */
  public Iterator<?> getElementIterator(int columnIndex);


  /**
   * Gets an iterator that walks through all rows from a particular column in this DDF, each time
   * returning a single element from that column.
   * 
   * @param rowType
   * @param columnType
   * @param columnName
   * @return
   */
  public <R, C> Iterator<C> getElementIterator(Class<R> rowType, Class<C> columnType, String columnName);

  /**
   * Gets an iterator that walks through all rows from a particular column in this DDF, each time
   * returning a single element from that column. Use default for the rowType & columnType.
   * 
   * @param columnName
   * @return
   */
  public Iterator<?> getElementIterator(String columnName);

  /**
   * @param numSamples
   * @return a new DDF containing `numSamples` rows selected randomly from our owner DDF.
   */
  public DDF getRandomSample(int numSamples);
}
