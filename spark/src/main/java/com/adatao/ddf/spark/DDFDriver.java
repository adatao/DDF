package com.adatao.ddf.spark;

import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;

import com.adatao.ddf.DDFContextManager;
import com.adatao.ddf.DDFFactory;
import com.adatao.ddf.exception.DDFException;

public class DDFContext implements com.adatao.ddf.DDFContext {
    static {
        try {
            DDFContextManager.registerDDFContext(new DDFContext());
        } catch (DDFException e) {
            throw new RuntimeException("Cannot register the Spark DDF Context.");
        }
    }
    
    @Override
    public boolean acceptURL(String connectionURL) throws DDFException {
        return true;
    }

    @Override
    public DDFFactory connect(String connectionURL,
            Map<String, String> connectionProps) throws DDFException {
        String[] jobJars = connectionProps.get("DDFSPARK_JAR").split(",");
        JavaSparkContext sc = new JavaSparkContext(connectionURL, "DDFClient", connectionProps.get("SPARK_HOME"), jobJars, connectionProps);
        return new SparkDDFFactory(sc);
    }

}
