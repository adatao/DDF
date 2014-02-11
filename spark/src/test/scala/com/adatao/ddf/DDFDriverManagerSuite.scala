package com.adatao.ddf

import com.adatao.ddf.DDFContext
import com.adatao.ddf.DDF
import com.adatao.ddf.DDFContextManager

class DDFContextManagerSuite extends ATestSuite {
  test("Test loading Spark Context Manager") {
    println("Test me")
    new com.adatao.ddf.spark.DDFContext
    val context = DDFContextManager.getContext("spark:")
    println(context)
  }
  
  test("Test connecting to a Spark Master") {
    println("Test me")
    new com.adatao.ddf.spark.DDFContext
    val connStr = "spark://ubuntu:7077"
    val context = DDFContextManager.getContext(connStr)
    val props = new java.util.HashMap[String, String]
    props.put("spark.home", System.getProperty("SPARK_HOME"))
    props.put("DDFSPARK_JAR", System.getenv.get("DDFSPARK_JAR"))
    println(props.get("DDFSPARK_JAR"))
    println(context)
    // two ways of getting DDFFactory
    // first, get it through ddf context
    val ddfFactory = context.connect(connStr, props)
    println(ddfFactory)
    
    // second, get it through Contextmanager directly.
    val ddfFactory2 = DDFContextManager.getDDFFactory(connStr, props)
  }
}
