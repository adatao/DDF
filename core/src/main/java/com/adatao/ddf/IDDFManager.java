package com.adatao.ddf;

import com.adatao.ddf.etl.IHandleSql;;


/**
 * 
 * @author ctn
 * 
 */
public interface IDDFManager extends IHandleSql {
  public void shutdown();
}
