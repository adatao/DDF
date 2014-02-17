/**
 * 
 */
package com.adatao.ddf.etl;

import com.adatao.ddf.ADDFFunctionalGroupHandler;
import com.adatao.ddf.ADDFManager;

/**
 * @author ctn
 * 
 */
public abstract class ASqlHandler extends ADDFFunctionalGroupHandler implements IHandleSql {

  public ASqlHandler(ADDFManager theDDFManager) {
    super(theDDFManager);
  }
}
