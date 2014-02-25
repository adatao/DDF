/**
 * 
 */
package com.adatao.ddf.types;

/**
 * @author ctn
 * 
 */
public class NA extends AMissingValueOK<Byte> {

  @Override
  protected String toStringWhenNotNull() {
    return "NA";
  }

  @Override
  public void setValue(Byte value) {
    // Do not allow setting to anything other than null
    mValue = null;
  }
}
