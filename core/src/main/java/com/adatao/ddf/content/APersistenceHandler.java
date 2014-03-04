/**
 * 
 */
package com.adatao.ddf.content;


import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.ADDFFunctionalGroupHandler;
import com.adatao.ddf.types.IGloballyAddressable;
import com.google.common.base.Strings;
import com.google.gson.annotations.Expose;

/**
 * 
 */
public abstract class APersistenceHandler extends ADDFFunctionalGroupHandler implements IHandlePersistence {

  public APersistenceHandler(DDF theDDF) {
    super(theDDF);
  }


  /**
   * The URI format should be:
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
   * @param uri
   * @return
   */

  public static class PersistenceUri {
    private String mEngine;
    private String mPath;


    public PersistenceUri(String uri) throws DDFException {
      if (Strings.isNullOrEmpty(uri)) throw new DDFException("uri may not be null or empty");

      String[] parts = uri.split("://");
      if (parts.length == 1) {
        mPath = parts[0];
      }

      else if (parts.length == 2) {
        mEngine = parts[0];
        mPath = parts[1];
      }
    }

    public PersistenceUri(String engine, String path) throws DDFException {
      mEngine = engine;
      mPath = path;
    }

    public String getEngine() {
      return mEngine;
    }

    protected void setEngine(String engine) {
      mEngine = engine;
    }

    public String getPath() {
      return mPath;
    }

    protected void setPath(String path) {
      mPath = path;
    }

    @Override
    public String toString() {
      return String.format("%s://%s", mEngine, mPath);
    }
  }


  /**
   * Base class for objects that can persist themselves, via the DDF persistence mechanism
   * 
   */
  public static abstract class APersistible implements IGloballyAddressable, IPersistible {

    private static final long serialVersionUID = -5941712506105779254L;


    /**
     * Each subclass is expected to instantiate a new DDF, put this {@link APersistible} object inside of it, and return
     * that DDF for persistence.
     * 
     * @return
     * @throws DDFException
     */
    protected abstract DDF newContainerDDFImpl() throws DDFException;


    // //// IPersistible /////

    private DDF newContainerDDF() throws DDFException {
      DDF ddf = this.newContainerDDFImpl();

      if (ddf == null) throw new DDFException(String.format("Cannot create new container DDF for %s: %s/%s",
          this.getClass(), this.getNamespace(), this.getName()));

      // Make sure we have a namespace and name
      if (Strings.isNullOrEmpty(this.getNamespace())) this.setNamespace(ddf.getNamespace());
      if (Strings.isNullOrEmpty(this.getName())) this.setName(ddf.getSchemaHandler().newTableName(this));

      // Make sure the DDF's names match ours
      ddf.setNamespace(this.getNamespace());
      ddf.setName(this.getName());

      return ddf;
    }

    @Override
    public PersistenceUri persist(boolean doOverwrite) throws DDFException {
      return this.newContainerDDF().persist(doOverwrite);
    }

    @Override
    public PersistenceUri persist() throws DDFException {
      return this.persist(true);
    }

    @Override
    public void unpersist() throws DDFException {
      this.newContainerDDF().unpersist();
    }


    // //// IGloballyAddressable //////

    @Expose private String mNamespace;
    @Expose private String mName;


    @Override
    public String getNamespace() {
      return mNamespace;
    }

    @Override
    public void setNamespace(String namespace) {
      mNamespace = namespace;
    }

    @Override
    public String getName() {
      return mName;
    }

    @Override
    public void setName(String name) {
      mName = name;
    }



    @Override
    public void afterSerialization() throws DDFException {}

    @Override
    public void beforeSerialization() throws DDFException {}

    @Override
    public ISerializable afterDeserialization(ISerializable deserializedObject, Object serializationData) throws DDFException {
      return deserializedObject;
    }
  }
}
