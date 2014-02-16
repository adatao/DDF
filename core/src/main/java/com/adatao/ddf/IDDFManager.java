package com.adatao.ddf;

import com.adatao.ddf.etl.IHandlePersistence;


/**
 * 
 * @author ctn
 * 
 */
public interface IDDFManager extends IHandlePersistence {
  public void shutdown();
}
