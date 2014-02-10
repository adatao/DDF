package com.adatao.ddf.spark;

import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;

import com.adatao.ddf.DDFDriverManager;
import com.adatao.ddf.DDFFactory;
import com.adatao.ddf.exception.DDFException;

public class DDFDriver implements com.adatao.ddf.DDFDriver {
    static {
        try {
            DDFDriverManager.registerDDFDriver(new DDFDriver());
        } catch (DDFException e) {
            throw new RuntimeException("Cannot register the Spark DDF Driver.");
        }
    }
    
    @Override
    public boolean acceptURL(String connectionURL) throws DDFException {
        return true;
    }

    @Override
    public DDFFactory connect(String connectionURL,
            Map<String, String> connectionProps) throws DDFException {
        String[] jobJars = connectionProps.get("RSERVER_JAR").split(",");
        JavaSparkContext sc = new JavaSparkContext(connectionURL, "DDFClient", connectionProps.get("SPARK_HOME"), jobJars, connectionProps);
        return new SparkDDFFactory(sc);
    }

}
