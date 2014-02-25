package com.adatao.ddf.util;

import java.io.File;
import java.math.BigDecimal;
import java.text.DecimalFormat;

/**
 * 
 * @author bhan
 * 
 */

public class Utils {

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
