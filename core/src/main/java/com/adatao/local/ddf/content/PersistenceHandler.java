/**
 * 
 */
package com.adatao.local.ddf.content;


import java.io.IOException;
import java.util.List;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDF.ConfigConstant;
import com.adatao.ddf.content.APersistenceHandler;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.util.Utils;
import com.adatao.ddf.util.Utils.JsonSerDes;
import com.google.common.base.Strings;

/**
 * This {@link PersistenceHandler} loads and saves from/to a designated local storage area.
 */
public class PersistenceHandler extends APersistenceHandler {

  public PersistenceHandler(DDF theDDF) {
    super(theDDF);
  }


  protected String locateOrCreatePersistenceDirectory() throws DDFException {
    String result = null, path = null;

    try {
      path = String.format("%s/%s", DDF.getConfigRuntimeDirectory(),
          DDF.getGlobalConfigValue(ConfigConstant.FIELD_LOCAL_PERSISTENCE_DIRECTORY));
      result = Utils.locateOrCreateDirectory(path);

    } catch (IOException e) {
      throw new DDFException(String.format("Unable to getPersistenceDirectory(%s)", path), e);
    }

    return result;
  }

  protected String locateOrCreatePersistenceSubdirectory(String subdir) throws DDFException {
    String result = null, path = null;

    try {
      path = String.format("%s/%s", this.locateOrCreatePersistenceDirectory(), subdir);
      result = Utils.locateOrCreateDirectory(path);

    } catch (IOException e) {
      throw new DDFException(String.format("Unable to getPersistenceSubdirectory(%s)", path), e);
    }

    return result;
  }

  protected String getDataFileName() throws DDFException {
    return this.getFilePath(this.getDDF().getNamespace(), this.getDDF().getName(), ".dat");
  }

  protected String getDataFileName(String namespace, String name) throws DDFException {
    return this.getFilePath(namespace, name, ".dat");
  }

  protected String getSchemaFileName() throws DDFException {
    return this.getFilePath(this.getDDF().getNamespace(), this.getDDF().getName(), ".sch");
  }

  protected String getSchemaFileName(String namespace, String name) throws DDFException {
    return this.getFilePath(namespace, name, ".sch");
  }

  private String getFilePath(String namespace, String name, String postfix) throws DDFException {
    String directory = locateOrCreatePersistenceSubdirectory(namespace);
    return String.format("%s/%s%s", directory, name, postfix);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.adatao.ddf.content.IHandlePersistence#save(boolean)
   */
  @Override
  public PersistenceUri persist(boolean doOverwrite) throws DDFException {
    if (this.getDDF() == null) throw new DDFException("DDF cannot be null");

    String dataFile = this.getDataFileName();
    String schemaFile = this.getSchemaFileName();

    if (!doOverwrite && (Utils.fileExists(dataFile) || Utils.fileExists(schemaFile))) {
      throw new DDFException("DDF already exists in persistence storage, and overwrite option is false");
    }

    try {
      Utils.writeToFile(dataFile, JsonSerDes.serialize(this.getDDF()) + '\n');
      Utils.writeToFile(schemaFile, JsonSerDes.serialize(this.getDDF().getSchema()) + '\n');

    } catch (Exception e) {
      throw new DDFException(e);
    }

    return new PersistenceUri(this.getDDF().getEngine(), dataFile);
  }



  /*
   * (non-Javadoc)
   * 
   * @see com.adatao.ddf.content.IHandlePersistence#delete(java.lang.String, java.lang.String)
   */
  @Override
  public void unpersist(String namespace, String name) throws DDFException {
    Utils.deleteFile(this.getDataFileName(namespace, name));
    Utils.deleteFile(this.getSchemaFileName(namespace, name));
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.adatao.ddf.content.IHandlePersistence#copy(java.lang.String, java.lang.String, java.lang.String,
   * java.lang.String, boolean)
   */
  @Override
  public void duplicate(String fromNamespace, String fromName, String toNamespace, String toName, boolean doOverwrite)
      throws DDFException {
    // TODO Auto-generated method stub

  }


  @Override
  public IPersistible load(String uri) throws DDFException {
    return (DDF) this.load(new PersistenceUri(uri));
  }

  @Override
  public IPersistible load(PersistenceUri uri) throws DDFException {
    PersistenceUri2 uri2 = new PersistenceUri2(uri);
    return this.load(uri2.getNamespace(), uri2.getName());
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.adatao.ddf.content.IHandlePersistence#load(java.lang.String, java.lang.String)
   */
  @Override
  public IPersistible load(String namespace, String name) throws DDFException {
    Object ddf = null, schema = null;

    try {
      ddf = JsonSerDes.loadFromFile(this.getFilePath(namespace, name, ".dat"));
      if (ddf == null) throw new DDFException((String.format("Got null for DDF for %s/%s", namespace, name)));

      schema = JsonSerDes.loadFromFile(this.getFilePath(namespace, name, ".sch"));
      if (schema == null) throw new DDFException((String.format("Got null for Schema for %s/%s", namespace, name)));

    } catch (Exception e) {
      throw new DDFException(String.format("Unable to load DDF or schema for %s/%s", namespace, name), e);
    }


    if (ddf instanceof DDF) {
      if (schema instanceof Schema) {
        ((DDF) ddf).getSchemaHandler().setSchema((Schema) schema);
      }

    } else {
      throw new DDFException("Expected object to be DDF, got " + ddf.getClass());
    }

    return (IPersistible) ddf;
  }

  @Override
  public List<String> listNamespaces() throws DDFException {
    return Utils.listSubdirectories(this.locateOrCreatePersistenceDirectory());
  }


  @Override
  public List<String> listItems(String namespace) throws DDFException {
    return Utils.listSubdirectories(this.locateOrCreatePersistenceSubdirectory(namespace));
  }


  /**
   * Like {@link PersistenceUri} but also with namespace and name parsed
   */
  public static class PersistenceUri2 extends PersistenceUri {
    public PersistenceUri2(String uri) throws DDFException {
      super(uri);
      this.parsePath();
    }

    public PersistenceUri2(PersistenceUri uri) throws DDFException {
      super(uri.getEngine(), uri.getPath());
      this.parsePath();
    }


    private String mNamespace;
    private String mName;


    /**
     * Parse the path part of the uri into namespace and name
     */
    private void parsePath() {
      if (Strings.isNullOrEmpty(this.getPath())) return;

      String[] parts = this.getPath().split("/");
      if (parts == null || parts.length == 0) return;

      String name = parts[parts.length - 1];
      if (!Strings.isNullOrEmpty(name)) {
        if (name.toLowerCase().endsWith(".dat") || name.toLowerCase().endsWith(".sch")) {
          name = name.substring(0, name.lastIndexOf('.'));
          // Also trim our current path
          this.setPath(this.getPath().substring(0, this.getPath().lastIndexOf('.')));
        }
      }
      this.setName(name);

      if (parts.length > 1) {
        this.setNamespace(parts[parts.length - 2]);
      }
    }

    /**
     * @return the namespace
     */
    public String getNamespace() {
      return mNamespace;
    }

    /**
     * @param namespace
     *          the namespace to set
     */
    protected void setNamespace(String namespace) {
      this.mNamespace = namespace;
    }

    /**
     * @return the name
     */
    public String getName() {
      return mName;
    }

    /**
     * @param name
     *          the name to set
     */
    protected void setName(String name) {
      this.mName = name;
    }
  }
}
