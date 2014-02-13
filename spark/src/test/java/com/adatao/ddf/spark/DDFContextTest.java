package com.adatao.ddf.spark;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.adatao.ddf.DDFContextManager;
import com.adatao.ddf.DDFContext;
import com.adatao.ddf.DDFContextFactory;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.DDF;

public class DDFContextTest {

    @Test
    public void testLongDDFContextRetrieval() throws DDFException {
        Map<String, String> env = System.getenv();
        String connStr = env.get("SPARK_MASTER");
        com.adatao.ddf.DDFContextFactory contextFactory = DDFContextManager.getContextFactory(connStr);
        Map<String, String> props = new HashMap<String, String>();
        props.put("spark.home", System.getProperty("SPARK_HOME"));
        props.put("DDFSPARK_JAR", env.get("DDFSPARK_JAR"));
        props.put("SPARK_MASTER", env.get("SPARK_MASTER"));
        System.out.println(System.getProperty("spark.serializer"));
        System.out.println(props.get("DDFSPARK_JAR"));
        System.out.println(contextFactory);
        DDFContext context = contextFactory.connect(connStr, props);
        System.out.println(context);
        ((SparkDDFContext) context).shutdown();
    }
    
    @Test
    public void testSimpleDDFContext() throws DDFException {
        Map<String, String> env = System.getenv();
        String connStr = env.get("SPARK_MASTER");
        DDFContext context = DDFContextManager.getDDFContext(connStr);
        System.out.println(context);
        // Now you can create DDF
        DDF ddf = context.fromSql("select * from airline");
        
        ((SparkDDFContext) context).shutdown();
    }

}
