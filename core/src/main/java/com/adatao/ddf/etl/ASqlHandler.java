/**
 * 
 */
package com.adatao.ddf.etl;

import com.adatao.ddf.DDF;
import com.adatao.ddf.misc.ADDFFunctionalGroupHandler;

/**
 * @author ctn
 * 
 */
public abstract class ASqlHandler extends ADDFFunctionalGroupHandler implements IHandleSql {

  public ASqlHandler(DDF theDDF) {
    super(theDDF);
  }
}
