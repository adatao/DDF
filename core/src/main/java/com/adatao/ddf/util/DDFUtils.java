package com.adatao.ddf.util;

import java.text.DecimalFormat;
/**
 * 
 * @author bhan
 *
 */

public class DDFUtils {

  public static double formatDouble(double number) {
    DecimalFormat fmt = new DecimalFormat("#.##");
    if (Double.isNaN(number)) {
      return Double.NaN;
    } else {
      return Double.parseDouble((fmt.format(number)));
    }

  }
}
