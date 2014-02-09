package com.adatao.ddf.spark

import com.adatao.ddf.DDF

/**
 * SparkDDF extends DDF in order to provide some utility methods
 * that are specific to Spark framework.
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 */
trait SparkDDF extends DDF {
  /**
   * We could add internal methods that only specific for Spark DDF here.
   * Such method should only be used within the spark module. They should not
   * be used outside of this module.
   */
}

/**
 * SparkDDF is an implementation of DDF using Spark and Shark framework.
 * @author Cuong Kien Bui
 * @version 0.1
 */
class SparkDDFImpl extends SparkDDF {
  def loadData(source: java.lang.String): DDF = {
    return null
  }
}

/**
 * Spark081DDFImpl is an implementation of SparkDDF that is specific to
 * Spark 0.8.1 version in order to use all useful features of Spark 0.8.1
 * to boost performance, or to deal with some specific issue that Spark 0.8.1
 * poses.
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 */
class Spark081DDFImpl extends SparkDDFImpl {
  
}
