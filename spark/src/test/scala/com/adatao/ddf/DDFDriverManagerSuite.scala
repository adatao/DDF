package com.adatao.ddf

import com.adatao.ddf.DDFContext
import com.adatao.ddf.DDF
import com.adatao.ddf.DDFContextManager

class DDFContextManagerSuite extends ATestSuite {
  test("Test loading Spark Context Manager") {
    println("Test me")
    new com.adatao.ddf.spark.DDFContextFactory
    val contextFactory = DDFContextManager.getContextFactory("spark:")
    println(contextFactory)
  }
  
  test("Test connecting to a Spark Master") {
    println("Test me")
    new com.adatao.ddf.spark.DDFContextFactory
    val connStr = "spark://ubuntu:7077"
    val contextFactory = DDFContextManager.getContextFactory(connStr)
    val props = new java.util.HashMap[String, String]
    props.put("spark.home", System.getProperty("SPARK_HOME"))
    props.put("DDFSPARK_JAR", System.getenv.get("DDFSPARK_JAR"))
    println(props.get("DDFSPARK_JAR"))
    println(contextFactory)
    // two ways of getting DDFFactory
    // first, get it through ddf context
    val context = contextFactory.connect(connStr, props)
    println(context)
    
    // second, get it through Contextmanager directly.
    val context2 = DDFContextManager.getDDFContext(connStr, props)
  }
}
