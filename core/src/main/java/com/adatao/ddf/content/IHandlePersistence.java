package com.adatao.ddf.content;


import java.util.List;
import com.adatao.ddf.DDF;
import com.adatao.ddf.IHandleDDFFunctionalGroup;
import com.adatao.ddf.content.APersistenceHandler.PersistenceUri;
import com.adatao.ddf.exception.DDFException;

public interface IHandlePersistence extends IHandleDDFFunctionalGroup {

  /**
   * Returns a list of existing namespaces we have in persistent storage
   * 
   * @return
   * @throws DDFException
   */
  public List<String> listNamespaces() throws DDFException;

  /**
   * Returns a list of existing DDFs we have in the given namespace in persistent storage
   * 
   * @param namespace
   * @return
   * @throws DDFException
   */
  public List<String> listDDFs(String namespace) throws DDFException;

  /**
   * Saves the current DDF to our default persistent storage, at minimum by namespace and name, together with the DDF's
   * metadata, and in whatever serialized format that can be read back in later.
   * 
   * @param doOverwrite
   *          overwrites if true
   * @return PersistenceUri of persisted location
   * @throws DDFException
   *           if doOverwrite is false and the destination already exists
   */
  public PersistenceUri save(boolean doOverwrite) throws DDFException;

  /**
   * Deletes the identified DDF "file" from persistent storage. This does not affect any DDF currently loaded in memory
   * or in processing space.
   * 
   * @param namespace
   * @param name
   * @throws DDFException
   *           if specified target does not exist
   */
  public void delete(String namespace, String name) throws DDFException;

  /**
   * Copies the identified DDF "files" from the specified source to destination.
   * 
   * @param fromNamespace
   * @param fromName
   * @param toNamespace
   * @param toName
   * @param doOverwrite
   *          overwrites if true
   * @throws DDFException
   *           if doOverwrite is false and the destination already exists
   */
  public void copy(String fromNamespace, String fromName, String toNamespace, String toName, boolean doOverwrite)
      throws DDFException;

  /**
   * Loads from default persistent storage into a DDF
   * 
   * @param namespace
   * @param name
   * @return
   * @throws DDFException
   *           , e.g., if file does not exist
   */
  public DDF load(String namespace, String name) throws DDFException;

  /**
   * Loads DDF from given URI. The URI format should be:
   * 
   * <pre>
   * <engine>://<path>
   * </pre>
   * 
   * e.g.,
   * 
   * <pre>
   * local:///root/ddf/ddf-runtime/local-ddf-db/com.example/MyDDF.dat
   * </pre>
   * 
   * If the engine is not specified, the current DDF's engine is used.
   * 
   * @param uri
   * @return
   * @throws DDFException
   */
  public DDF load(String uri) throws DDFException;

  public DDF load(PersistenceUri uri) throws DDFException;
}
