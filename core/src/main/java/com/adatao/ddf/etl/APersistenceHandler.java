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
public abstract class APersistenceHandler extends ADDFFunctionalGroupHandler implements IHandlePersistence {

  public APersistenceHandler(ADDFManager theContainer) {
    super(theContainer);
  }

  protected void resetRepresentationsAndViews() {
    // this.getDDF().getHelper().getRepresentationHandler().reset();
    // this.getDDF().getHelper().getViewHandler().reset();
  }

  /**
   * The base implementation simply clears out all existing representations and views
   */
  @Override
  public void sqlLoad(String connection, String command) {
    this.load("SQL", connection, command);
  }

  @Override
  public void sqlSave(String connection, String command) {
    this.save("SQL", connection, command);
  }

  /**
   * The base implementation simply clears out all existing representations and views
   */
  @Override
  public void jdbcLoad(String connection, String command) {
    this.load("JDBC", connection, command);
  }

  @Override
  public void jdbcSave(String connection, String command) {
    this.save("JDBC", connection, command);
  }

  /**
   * The base implementation simply clears out all existing representations and views
   */
  @Override
  public void load(String type, String connection, String command) {
    this.resetRepresentationsAndViews();
  }

  @Override
  public void save(String type, String connection, String command) {
    // TODO Auto-generated method stub
  }

}
