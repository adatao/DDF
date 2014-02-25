package com.adatao.ddf.etl;

import java.util.List;

import com.adatao.ddf.DDF;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.content.Schema.DataFormat;
import com.adatao.ddf.exception.DDFException;

public interface IHandleSqlLike {

  /**
   * Executes the given command and stores the results into a DDF which already has a schema to
   * govern the data loading, from the system default data source, using its default
   * {@link DataFormat}.
   * 
   * @return the DDF with loaded data content
   */
  public DDF sql2ddf(String command) throws DDFException;

  /**
   * Executes the given command and stores the results into a DDF using the given {@link Schema},
   * from the system default data source.
   * 
   * @param command
   * @param schema
   * @return
   */
  public DDF sql2ddf(String command, Schema schema) throws DDFException;

  /**
   * Executes the given command and stores the results into a DDF, using the specified
   * {@link DataFormat}, from the system default data source.
   * 
   * @param command
   * @param dataFormat
   * @return
   */
  public DDF sql2ddf(String command, DataFormat dataFormat) throws DDFException;

  /**
   * Executes the given command and stores the results into a DDF using the given {@link Schema},
   * from the specified source. The {@link DataFormat} is assumed to be whatever is the default
   * provided by the data source.
   * 
   * @param command
   * @param schema
   *          If {@link Schema} is null, then the data is expected to have {@link Schema}
   *          information available
   * @param dataSource
   *          The dataSource (URI) of the data, e.g., jdbc://xxx
   * @return
   */
  public DDF sql2ddf(String command, Schema schema, String dataSource) throws DDFException;

  /**
   * Executes the given command and stores the results into a DDF using the given {@link Schema},
   * from the specified dataSource.
   * 
   * @param command
   * @param schema
   *          If schema is null, then the data is expected to have schema information available
   * @param dataFormat
   * @return
   */
  public DDF sql2ddf(String command, Schema schema, DataFormat dataFormat) throws DDFException;

  /**
   * Executes the given command and stores the results into a DDF using the given {@link Schema},
   * from the specified dataSource.
   * 
   * @param command
   * @param schema
   *          If {@link Schema} is null, then the data is expected to have {@link Schema}
   *          information available
   * @param dataSource
   *          The dataSource (URI) of the data, e.g., jdbc://xxx
   * @param dataFormat
   * @return
   */
  public DDF sql2ddf(String command, Schema schema, String dataSource, DataFormat dataFormat) throws DDFException;



  /**
   * Executes the given command and stores the results into a List<String> which already has a
   * schema to govern the data loading, from the system default data source, using its default
   * {@link DataFormat}.
   * 
   * @return the List<String> with loaded data content
   */
  public List<String> sql2txt(String command) throws DDFException;

  /**
   * Executes the given command and stores the results into a List<String> using the given
   * {@link Schema}, from the specified source. The {@link DataFormat} is assumed to be whatever is
   * the default provided by the data source.
   * 
   * @param command
   * @param dataSource
   *          The dataSource (URI) of the data, e.g., jdbc://xxx
   * @return
   */
  public List<String> sql2txt(String command, String dataSource) throws DDFException;
}
