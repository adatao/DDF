package com.adatao.ddf.types;

/**
 * Abstract base type for support of missing values (NAs) in the data
 * 
 * @author ctn
 * 
 */
public abstract class AMissingValueOK<T> {
  protected T mValue;

  public T getValue() {
    return mValue;
  }

  public void setValue(T value) {
    mValue = value;
  }

  public boolean isNA() {
    return mValue == null;
  }

  public boolean isMissing() {
    return this.isNA();
  }

  protected String toStringWhenNotNull() {
    return mValue.toString();
  }

  @Override
  public String toString() {
    return mValue != null ? this.toStringWhenNotNull() : "NA";
  }
}
