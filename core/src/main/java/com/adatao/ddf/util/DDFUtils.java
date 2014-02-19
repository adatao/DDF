package com.adatao.ddf.util;

import java.text.DecimalFormat;

public class DDFUtils {

  public static double formatDouble(double number) {
    DecimalFormat fmt = new DecimalFormat("#.##");
    return Double.parseDouble((fmt.format(number)));
  }
  public boolean isNA(String str) {
    return str.trim().toUpperCase().equals("NA");
  }
  
}
