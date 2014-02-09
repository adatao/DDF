package com.adatao.ddf;

import java.util.Iterator;
import java.util.Vector;
import java.util.ServiceLoader;

import com.adatao.ddf.exception.DDFException;

/**
 * A DDF Driver Manager help creating correct DDF Driver from
 * the specified connection strings.
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 *
 */
public class DDFDriverManager {
    private static Vector<DDFDriverInfo> driverList = new Vector<DDFDriverInfo>();
    
    public static DDFDriver getDriver(String connectionURL) 
            throws DDFException {
        try {
            Class.forName("com.adatao.ddf.spark.SparkDDFDriver");
        } catch (ClassNotFoundException e) {
            System.out.println(e);
        }
        
        for (DDFDriverInfo driverInfo: driverList) {
            try {
                if (driverInfo.driver.acceptURL(connectionURL)) {
                    return driverInfo.driver;
                }
            } catch (DDFException e) {
                e.printStackTrace();
            }
        }
        
        throw new DDFException("Cannot find any DDF drivers that can handle the connection string: "
                + connectionURL);
    }
    
    private static void loadDDFDrivers() {
        // use class loader to load all the available DDFDrivers into
        // memory.
        ServiceLoader<DDFDriver> loadedDrivers = ServiceLoader.load(DDFDriver.class);
        Iterator<DDFDriver> driversIterator = loadedDrivers.iterator();

        try{
            while(driversIterator.hasNext()) {
                System.out.println(" Loading done by the java.util.ServiceLoader :  "+driversIterator.next());
            }
        } catch(Throwable t) {
        // Do nothing
        }
    }
    
    /**
     * Register a DDF driver with the DriverManager.
     * 
     * @param driver
     */
    public static void registerDDFDriver(DDFDriver driver) {
        synchronized (DDFDriverManager.class) {
            DDFDriverInfo info = new DDFDriverInfo();
            info.driver = driver;
            info.driverClass = driver.getClass();
            info.driverClassName = driver.getClass().getName();
                    
            driverList.add(info);
        }
    }
}

class DDFDriverInfo {
    DDFDriver driver;
    Class driverClass;
    String driverClassName;
}