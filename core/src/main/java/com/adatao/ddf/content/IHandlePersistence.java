package com.adatao.ddf.content;

import java.io.IOException;

import com.adatao.ddf.DDF;
import com.adatao.ddf.IHandleDDFFunctionalGroup;

public interface IHandlePersistence extends IHandleDDFFunctionalGroup {

  /**
   * Saves the current DDF to our default persistent storage, at minimum by namespace and name,
   * together with the DDF's metadata, and in whatever serialized format that can be read back in
   * later.
   * 
   * @param doOverwrite
   *          overwrites if true
   * @throws IOException
   *           if doOverwrite is false and the destination already exists
   */
  public void save(boolean doOverwrite) throws IOException;

  /**
   * Deletes the identified DDF "file" from persistent storage. This does not affect any DDF
   * currently loaded in memory or in processing space.
   * 
   * @param namespace
   * @param name
   * @throws IOException
   *           if specified target does not exist
   */
  public void delete(String namespace, String name) throws IOException;

  /**
   * Copies the identified DDF "files" from the specified source to destination.
   * 
   * @param fromNamespace
   * @param fromName
   * @param toNamespace
   * @param toName
   * @param doOverwrite
   *          overwrites if true
   * @throws IOException
   *           if doOverwrite is false and the destination already exists
   */
  public void copy(String fromNamespace, String fromName, String toNamespace, String toName, boolean doOverwrite)
      throws IOException;

  /**
   * Loads from default persistent storage into a DDF
   * 
   * @param namespace
   * @param name
   * @return
   * @throws IOException
   *           , e.g., if file does not exist
   */
  public DDF load(String namespace, String name) throws IOException;
}
