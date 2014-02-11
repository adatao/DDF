package com.adatao.ddf.spark;

import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;

import com.adatao.ddf.DDFContextManager;
import com.adatao.ddf.DDFContext;
import com.adatao.ddf.exception.DDFException;

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
    public DDFContext connect(String connectionURL,
            Map<String, String> connectionProps) throws DDFException {
        String[] jobJars = connectionProps.get("DDFSPARK_JAR").split(",");
        JavaSparkContext sc = new JavaSparkContext(connectionURL, "DDFClient", connectionProps.get("SPARK_HOME"), jobJars, connectionProps);
        return new SparkDDFContext(sc);
    }

}
