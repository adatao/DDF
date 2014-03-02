/**
 * 
 */
package com.adatao.ddf.content;


import com.adatao.ddf.ADDFFunctionalGroupHandler;
import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.google.common.base.Strings;

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
}
