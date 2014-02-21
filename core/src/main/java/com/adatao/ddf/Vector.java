/**
 * 
 */
package com.adatao.ddf;

import java.util.UUID;

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
 * @author ctn
 * 
 */
public class Vector {

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
   * Instantiate a new Vector with the given Integer array
   * 
   * @param data
   * @param theColumnName
   * @throws DDFException
   */
  public Vector(String name, Integer[] data) throws DDFException {
    this.initialize(name, data);
  }

  /**
   * Instantiate a new Vector with the given Double array
   * 
   * @param data
   * @param theColumnName
   * @throws DDFException
   */
  public Vector(String name, Double[] data) throws DDFException {
    this.initialize(name, data);
  }

  /**
   * Instantiate a new Vector with the given String array
   * 
   * @param data
   * @param theColumnName
   * @throws DDFException
   */
  public Vector(String name, String[] data) throws DDFException {
    this.initialize(name, data);
  }

  private void initialize(String name, Object[] data) throws DDFException {
    DDF newDDF = DDF.newDDF(String.format("Vector-%", UUID.randomUUID()), new Schema.ColumnWithData(name, data));
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

}
