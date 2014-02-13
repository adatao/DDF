package com.adatao.ddf;


/**
 * 
 * @author ctn
 * 
 */
public interface IDDFManager {
  public enum DataFormat {
    SQL, CSV, TSV
  }

  /**
   * Loads data content into a DDF which already has a schema to govern the data loading, from the
   * system default source.
   * 
   * @return the DDF with loaded data content
   */
  public DDF load(String command);

  /**
   * Loads data content into a DDF using the given schema, from the system default source.
   * 
   * @param command
   * @param schema
   * @return
   */
  public DDF load(String command, String schema);

  /**
   * Loads data content into a DDF using the given schema, from the specified source.
   * 
   * @param command
   * @param schema
   *          If schema is null, then the data is expected to have schema information available
   * @param source
   *          If source is null, then use the system-default source
   * @return
   */
  public DDF load(String command, String schema, String source);

  /**
   * Loads data content into a DDF using the given schema, from the specified source.
   * 
   * @param command
   * @param schema
   *          If schema is null, then the data is expected to have schema information available
   * @param source
   *          If source is null, then use the system-default source
   * @param format
   * @return
   */
  public DDF load(String command, String schema, String source, DataFormat format);

  public void shutdown();

}
