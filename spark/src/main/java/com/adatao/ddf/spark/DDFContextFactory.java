package com.adatao.ddf.spark;

import java.util.Map;
import java.util.HashMap;

import org.apache.spark.api.java.JavaSparkContext;

import com.adatao.ddf.DDFContextManager;
import com.adatao.ddf.DDFContext;
import com.adatao.ddf.exception.DDFException;

/**
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 * 
 */
public class DDFContextFactory implements com.adatao.ddf.DDFContextFactory {
    static {
        try {
            DDFContextManager.registerDDFContextFactory(new DDFContextFactory());
        } catch (DDFException e) {
            throw new RuntimeException("Cannot register the Spark DDF Context.");
        }
    }
    
    @Override
    public boolean acceptURL(String connectionURL) throws DDFException {
        return true;
    }

    @Override
    public DDFContext connect(String connectionURL) throws DDFException {
        try {
            Map<String, String> env = System.getenv();
            Map<String, String> props = new HashMap<String, String>();
            props.put("spark.home", env.get("SPARK_HOME"));
            props.put("DDFSPARK_JAR", env.get("DDFSPARK_JAR"));
            
            String[] jobJars = props.get("DDFSPARK_JAR").split(",");
            JavaSparkContext sc = new JavaSparkContext(connectionURL, "DDFClient", props.get("SPARK_HOME"), jobJars, props);
            return new SparkDDFContext(sc);
        } catch (Exception e) {
            throw new DDFException(e);
        }
    }

    @Override
    public DDFContext connect(String connectionURL,
            Map<String, String> connectionProps) throws DDFException {
        try {
            String[] jobJars = connectionProps.get("DDFSPARK_JAR").split(",");
            JavaSparkContext sc = new JavaSparkContext(connectionURL, "DDFClient", connectionProps.get("SPARK_HOME"), jobJars, connectionProps);
            return new SparkDDFContext(sc);
        } catch (Exception e) {
            throw new DDFException(e);
        }
    }

}
