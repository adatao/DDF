/**
 * 
 */
package com.adatao.ddf;


import java.util.Iterator;

import com.adatao.ddf.content.Schema;
import com.adatao.ddf.exception.DDFException;

/**
 * A one-dimensional array of values of the same type, e.g., Integer or Double or String.
 * <p>
 * We implement a Vector as simply a reference to a column in a DDF. The DDF may have a single
 * column, or multiple columns.
 * <p>
 * The column is referenced by name.
 * 
 * TODO: Vector operations
 * 
 * @author ctn
 * 
 */
public class Vector<T> {

  /**
   * Instantiate a new Vector based on an existing DDF, given a column name. The column name is not
   * verified for correctness; any errors would only show up on actual usage.
   * 
   * @param theDDF
   * @param theColumnName
   */
  public Vector(DDF theDDF, String theColumnName) {
    this.initialize(theDDF, theColumnName);
  }

  /**
   * Instantiate a new Vector with the given T array. Uses the default engine.
   * 
   * @param data
   * @param theColumnName
   * @throws DDFException
   */
  public Vector(String name, T[] data) throws DDFException {
    this.initialize(name, data, null);
  }

  /**
   * Instantiate a new Vector with the given T array. Uses the default engine.
   * 
   * @param data
   * @param theColumnName
   * @param engineName
   * @throws DDFException
   */
  public Vector(String name, T[] data, String engineName) throws DDFException {
    this.initialize(name, data, engineName);
  }

  private void initialize(String name, T[] data, String engineName) throws DDFException {
    if (data == null || data.length == 0) throw new DDFException("Cannot initialize a null or zero-length Vector");

    Class<?> rowType = data[0].getClass();
    DDF newDDF = DDFManager.get(engineName).newDDF(null, (Object) data, rowType, null, name, new Schema(name));

    this.initialize(newDDF, name);
  }

  private void initialize(DDF theDDF, String name) {
    this.setDDF(theDDF);
    this.setDDFColumnName(name);
  }


  /**
   * The DDF that contains this vector
   */
  private DDF mDDF;

  /**
   * The name of the DDF column we are pointing to
   */
  private String mDDFColumnName;

  /**
   * @return the mDDF
   */
  public DDF getDDF() {
    return mDDF;
  }

  /**
   * @param mDDF
   *          the mDDF to set
   */
  public void setDDF(DDF mDDF) {
    this.mDDF = mDDF;
  }

  /**
   * @return the mDDFColumnName
   */
  public String getDDFColumnName() {
    return mDDFColumnName;
  }

  /**
   * @param mDDFColumnName
   *          the mDDFColumnName to set
   */
  public void setDDFColumnName(String mDDFColumnName) {
    this.mDDFColumnName = mDDFColumnName;
  }

  @SuppressWarnings("unchecked")
  public Iterator<T> iterator() {
    return (Iterator<T>) this.getDDF().getElementIterator(this.getDDFColumnName());
  }
}
