/**
 * 
 */
package com.adatao.ddf.etl;

import com.adatao.ddf.ADDFFunctionalGroupHandler;
import com.adatao.ddf.DDF;

/**
 * @author ctn
 * 
 */
public abstract class ASqlHandler extends ADDFFunctionalGroupHandler implements IHandleSql {

  public ASqlHandler(DDF theDDF) {
    super(theDDF);
  }
}
