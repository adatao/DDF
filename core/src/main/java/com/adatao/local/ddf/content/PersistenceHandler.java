/**
 * 
 */
package com.adatao.local.ddf.content;


import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDF.ConfigConstant;
import com.adatao.ddf.content.APersistenceHandler;
import com.adatao.ddf.content.IBeforeAndAfterSerDes;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.util.Utils;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

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
    return this.getx(this.getDDF().getNamespace(), this.getDDF().getName(), ".dat");
  }

  protected String getDataFileName(String namespace, String name) throws DDFException {
    return this.getx(namespace, name, ".dat");
  }

  protected String getSchemaFileName() throws DDFException {
    return this.getx(this.getDDF().getNamespace(), this.getDDF().getName(), ".dat");
  }

  protected String getSchemaFileName(String namespace, String name) throws DDFException {
    return this.getx(namespace, name, ".sch");
  }

  private String getx(String namespace, String name, String postfix) throws DDFException {
    String directory = locateOrCreatePersistenceSubdirectory(namespace);
    return String.format("%s/%s%s", directory, name, postfix);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.adatao.ddf.content.IHandlePersistence#save(boolean)
   */
  @Override
  public String save(boolean doOverwrite) throws DDFException {
    if (this.getDDF() == null) throw new DDFException("DDF cannot be null");

    String dataFile = this.getDataFileName();
    String schemaFile = this.getSchemaFileName();

    if (!doOverwrite && (Utils.fileExists(dataFile) || Utils.fileExists(schemaFile))) {
      throw new DDFException("DDF already exists in persistence storage, and overwrite option is false");
    }

    this.writeToFile(dataFile, jsonSerialize(this.getDDF()));
    this.writeToFile(schemaFile, jsonSerialize(this.getDDF().getSchema()));

    return dataFile;
  }

  private void writeToFile(String fileName, String contents) throws DDFException {
    Writer writer = null;

    try {
      writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName), "utf-8"));
      writer.write(contents);
      writer.write('\n');

    } catch (IOException ex) {
      throw new DDFException(String.format("Cannot write to file %s", fileName, ex));

    } finally {
      try {
        writer.close();

      } catch (Exception ex) {
        mLog.error("While trying to call write.close()", ex);
      }
    }
  }


  protected static final String SERDES_CLASS_NAME_FIELD = "_class";
  protected static final String SERDES_TIMESTAMP_FIELD = "_timestamp";
  protected static final String SERDES_USER_FIELD = "_user";


  protected static <T> String jsonSerialize(Object ddf) throws DDFException {
    if (ddf == null) return "null";

    if (ddf instanceof IBeforeAndAfterSerDes) ((IBeforeAndAfterSerDes) ddf).beforeSerialization();

    Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
    String json = gson.toJson(ddf);

    // Add the bookkeeping fields, e.g., SERDES_CLASS_NAME_FIELD
    JsonObject jObj = toJsonObject(json);
    jObj.addProperty(SERDES_CLASS_NAME_FIELD, ddf.getClass().getSimpleName());
    jObj.addProperty(SERDES_TIMESTAMP_FIELD,
        DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.LONG).format(new Date()));
    jObj.addProperty(SERDES_USER_FIELD, System.getProperty("user.name"));


    json = gson.toJson(jObj);
    return json;
  }

  private static JsonObject toJsonObject(String json) {
    JsonElement jElement = new JsonParser().parse(json);
    if (jElement == null) return null;

    JsonObject jObj = jElement.getAsJsonObject();
    return jObj;
  }

  protected static DDF jsonDeserialize(String json) throws DDFException {
    if (Strings.isNullOrEmpty(json)) return null;

    try {
      JsonElement jElement = new JsonParser().parse(json);
      if (jElement == null) return null;

      JsonObject jObj = jElement.getAsJsonObject();
      if (jObj == null) return null;

      jElement = jObj.get(SERDES_CLASS_NAME_FIELD);
      if (jElement == null) return null;

      String className = jElement.getAsString();
      if (Strings.isNullOrEmpty(className)) return null;

      Class<?> theClass = Class.forName(className);
      if (theClass == null) return null;

      Object obj = new Gson().fromJson(json, theClass);

      if (obj instanceof IBeforeAndAfterSerDes) ((IBeforeAndAfterSerDes) obj).afterDeserialization();

      return (obj instanceof DDF ? (DDF) obj : null);

    } catch (Exception e) {
      throw new DDFException("Cannot deserialize " + json, e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.adatao.ddf.content.IHandlePersistence#delete(java.lang.String, java.lang.String)
   */
  @Override
  public void delete(String namespace, String name) throws DDFException {
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
  public void copy(String fromNamespace, String fromName, String toNamespace, String toName, boolean doOverwrite)
      throws DDFException {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.adatao.ddf.content.IHandlePersistence#load(java.lang.String, java.lang.String)
   */
  @Override
  public DDF load(String namespace, String name) throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }


  @Override
  public List<String> listNamespaces() throws DDFException {
    return Utils.listSubdirectories(this.locateOrCreatePersistenceDirectory());
  }


  @Override
  public List<String> listDDFs(String namespace) throws DDFException {
    return Utils.listSubdirectories(this.locateOrCreatePersistenceSubdirectory(namespace));
  }
}
