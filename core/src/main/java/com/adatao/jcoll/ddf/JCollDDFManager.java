/**
 * 
 */
package com.adatao.jcoll.ddf;

import com.adatao.ddf.DDFManager;
import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;

/**
 * @author ctn
 * 
 */
public class JCollDDFManager extends DDFManager {

  @Override
  public String getEngine() {
    return "java_collections";
  }

  @Override
  protected DDF createDummyDDF() throws DDFException {
    return new DDF(this, null, null, null, null, null);
  }


}
