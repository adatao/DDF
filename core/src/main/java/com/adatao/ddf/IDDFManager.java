package com.adatao.ddf;

import com.adatao.ddf.etl.IHandleDataCommands;


/**
 * 
 * @author ctn
 * 
 */
public interface IDDFManager extends IHandleDataCommands {
  public void shutdown();
}
