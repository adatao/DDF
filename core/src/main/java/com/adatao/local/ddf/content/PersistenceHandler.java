/**
 * 
 */
package com.adatao.local.ddf.content;


import java.io.IOException;
import com.adatao.ddf.DDF;
import com.adatao.ddf.content.APersistenceHandler;
import com.adatao.ddf.util.ConfigHandler.ConfigConstant;
import com.adatao.ddf.util.Utils;

/**
 * This {@link PersistenceHandler} loads and saves from/to a designated local storage area.
 */
public class PersistenceHandler extends APersistenceHandler {

  public PersistenceHandler(DDF theDDF) {
    super(theDDF);
  }


  protected String getPersistenceDirectory() throws IOException {
    String persistenceDir = Utils.locateOrCreateDirectory(String.format("%s/%s", //
        DDF.getConfigRuntimeDirectory(), DDF.getGlobalConfigValue(ConfigConstant.FIELD_LOCAL_PERSISTENCE_DIRECTORY)));

    return persistenceDir;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.adatao.ddf.content.IHandlePersistence#save(boolean)
   */
  @Override
  public void save(boolean doOverwrite) throws IOException {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.adatao.ddf.content.IHandlePersistence#delete(java.lang.String, java.lang.String)
   */
  @Override
  public void delete(String namespace, String name) throws IOException {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.adatao.ddf.content.IHandlePersistence#copy(java.lang.String, java.lang.String, java.lang.String,
   * java.lang.String, boolean)
   */
  @Override
  public void copy(String fromNamespace, String fromName, String toNamespace, String toName, boolean doOverwrite)
      throws IOException {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.adatao.ddf.content.IHandlePersistence#load(java.lang.String, java.lang.String)
   */
  @Override
  public DDF load(String namespace, String name) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

}
