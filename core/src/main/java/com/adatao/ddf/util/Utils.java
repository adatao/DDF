package com.adatao.ddf.util;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.ddf.content.IBeforeAndAfterSerDes;
import com.adatao.ddf.exception.DDFException;
import com.adatao.local.ddf.LocalObjectDDF;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import scala.actors.threadpool.Arrays;

/**
 * 
 */

public class Utils {

  public static Logger sLog = LoggerFactory.getLogger(Utils.class);


  public static List<String> listFiles(String directory) {
    return listDirectory(directory, true, false);
  }

  public static List<String> listSubdirectories(String directory) {
    return listDirectory(directory, false, true);
  }

  public static List<String> listDirectory(String directory) {
    return listDirectory(directory, true, true);
  }

  @SuppressWarnings("unchecked")
  private static List<String> listDirectory(String directory, final boolean doIncludeFiles,
      final boolean doIncludeSubdirectories) {

    String[] directories = new File(directory).list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if (name.startsWith(".")) return false; // HACK: auto-exclude Unix hidden files

        File item = new File(dir, name);
        if (doIncludeFiles && item.isFile()) return true;
        if (doIncludeSubdirectories && item.isDirectory()) return true;
        return false;
      }
    });

    return Arrays.asList(directories);
  }



  /**
   * Locates the given dirName as a full path, in the current directory or in successively higher parent directory
   * above.
   * 
   * @param dirName
   * @return
   * @throws IOException
   */
  public static String locateDirectory(String dirName) throws IOException {
    if (Utils.dirExists(dirName)) return dirName;

    String path = null;
    String curDir = new File(".").getCanonicalPath();

    // Go for at most 10 levels up
    for (int i = 0; i < 10; i++) {
      path = String.format("%s/%s", curDir, dirName);
      if (Utils.dirExists(path)) break;
      curDir = String.format("%s/..", curDir);
    }

    if (path != null) {
      File file = new File(path);
      path = file.getCanonicalPath();
      if (!Utils.dirExists(path)) path = null;
    }


    return path;
  }

  /**
   * Same as locateDirectory(dirName), but also creates it if it doesn't exist.
   * 
   * @param dirName
   * @return
   * @throws IOException
   */
  public static String locateOrCreateDirectory(String dirName) throws IOException {
    String path = locateDirectory(dirName);

    if (path == null) {
      File file = new File(dirName);
      file.mkdirs();
      path = file.getCanonicalPath();
    }

    return path;
  }

  /**
   * 
   * @param path
   * @return true if "path" exists and is a file (and not a directory)
   */
  public static boolean fileExists(String path) {
    File f = new File(path);
    return (f.exists() && !f.isDirectory());
  }

  /**
   * 
   * @param path
   * @return true if "path" exists and is a directory (and not a file)
   */
  public static boolean dirExists(String path) {
    File f = new File(path);
    return (f.exists() && f.isDirectory());
  }

  public static double formatDouble(double number) {
    DecimalFormat fmt = new DecimalFormat("#.##");
    if (Double.isNaN(number)) {
      return Double.NaN;
    } else {
      return Double.parseDouble((fmt.format(number)));
    }
  }

  public static double round(double number, int precision, int mode) {
    BigDecimal bd = new BigDecimal(number);
    return bd.setScale(precision, mode).doubleValue();
  }

  public static double roundUp(double number) {
    if (Double.isNaN(number)) {
      return Double.NaN;
    } else {
      return round(number, 2, BigDecimal.ROUND_HALF_UP);
    }
  }

  public static void deleteFile(String fileName) {
    new File(fileName).delete();
  }

  public static String readFromFile(String fileName) throws IOException {
    Reader reader = null;

    try {
      reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), "utf-8"));
      return IOUtils.toString(reader);

    } catch (IOException ex) {
      throw new IOException(String.format("Cannot read from file %s", fileName, ex));

    } finally {
      reader.close();
    }
  }

  public static void writeToFile(String fileName, String contents) throws IOException {
    Writer writer = null;

    try {
      writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName), "utf-8"));
      writer.write(contents);

    } catch (IOException ex) {
      throw new IOException(String.format("Cannot write to file %s", fileName, ex));

    } finally {
      writer.close();
    }
  }


  public static class JsonSerDes {

    public static final String SERDES_CLASS_NAME_FIELD = "_class";
    public static final String SERDES_TIMESTAMP_FIELD = "_timestamp";
    public static final String SERDES_USER_FIELD = "_user";


    public static String serialize(Object obj) throws DDFException {
      if (obj == null) return "null";

      if (obj instanceof IBeforeAndAfterSerDes) ((IBeforeAndAfterSerDes) obj).beforeSerialization();

      Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
      String json = gson.toJson(obj);

      // Add the bookkeeping fields, e.g., SERDES_CLASS_NAME_FIELD
      JsonObject jObj = toJsonObject(json);
      jObj.addProperty(SERDES_CLASS_NAME_FIELD, obj.getClass().getCanonicalName());
      jObj.addProperty(SERDES_TIMESTAMP_FIELD, DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.LONG)
          .format(new Date()));
      jObj.addProperty(SERDES_USER_FIELD, System.getProperty("user.name"));


      json = gson.toJson(jObj);
      return json;
    }

    public static JsonObject toJsonObject(String json) {
      JsonElement jElement = new JsonParser().parse(json);
      if (jElement == null) return null;

      JsonObject jObj = jElement.getAsJsonObject();
      return jObj;
    }

    public static Object deserialize(String json) throws DDFException {
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

        return obj;

      } catch (Exception e) {
        throw new DDFException("Cannot deserialize " + json, e);
      }
    }

    public static Object loadFromFile(String path) throws IOException {
      String json = Utils.readFromFile(path);

      // First determine the object's class
      JsonObject jsonObj = toJsonObject(json);

      String objClassName = jsonObj.get(SERDES_CLASS_NAME_FIELD).getAsString();
      if (Strings.isNullOrEmpty(objClassName)) objClassName = LocalObjectDDF.class.getName();

      Class<?> objClass = null;
      try {
        objClass = Class.forName(objClassName);
      } catch (ClassNotFoundException e) {
        Utils.sLog.warn(String.format("Unable to load class %s", objClassName), e);
      }
      if (objClass == null) objClass = LocalObjectDDF.class;

      // Now deserialize
      Object obj = new Gson().fromJson(json, objClass);
      return obj;
    }
  }
}
