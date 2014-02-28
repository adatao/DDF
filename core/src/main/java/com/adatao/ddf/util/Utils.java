package com.adatao.ddf.util;


import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */

public class Utils {

  public Logger sLog = LoggerFactory.getLogger(Utils.class);


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
}
