package com.adatao.ddf.etl;

import com.adatao.ddf.DDF;

public interface IHandlePersistence {

  /**
   * Perform a SQL load into the DDF
   * 
   * @param connection
   * @param command
   */
  public void sqlLoad(String connection, String command);

  /**
   * Perform a SQL save from the DDF
   * 
   * @param connection
   * @param command
   */
  public void sqlSave(String connection, String command);

  /**
   * Perform a JDBC load into the DDF
   * 
   * @param connection
   * @param command
   */
  public void jdbcLoad(String connection, String command);

  /**
   * Perform a JDBC save from the DDF
   * 
   * @param connection
   * @param command
   */
  public void jdbcSave(String connection, String command);

  /**
   * Perform a load of the given "source type" into the DDF
   * 
   * @param type
   * @param connection
   * @param command
   */
  public void load(String type, String connection, String command);

  /**
   * Perform a save of the current "destination type" from the DDF
   * 
   * @param type
   * @param connection
   * @param command
   */
  public void save(String type, String connection, String command);

}
