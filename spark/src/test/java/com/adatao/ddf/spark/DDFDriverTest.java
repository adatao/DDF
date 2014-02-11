package com.adatao.ddf.spark;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.adatao.ddf.DDFDriverManager;
import com.adatao.ddf.DDFFactory;
import com.adatao.ddf.exception.DDFException;

public class DDFDriverTest {

    @Test
    public void test() throws DDFException {
        String connStr = "spark://ubuntu:7077";
        com.adatao.ddf.DDFDriver driver = DDFDriverManager.getDriver(connStr);
        Map<String, String> env = System.getenv();
        Map<String, String> props = new HashMap<String, String>();
        props.put("spark.home", System.getProperty("SPARK_HOME"));
        props.put("DDFSPARK_JAR", env.get("DDFSPARK_JAR"));
        System.out.println(System.getProperty("spark.serializer"));
        System.out.println(props.get("DDFSPARK_JAR"));
        System.out.println(driver);
        DDFFactory factory = driver.connect(connStr, props);
        System.out.println(factory);
        
    }

}
