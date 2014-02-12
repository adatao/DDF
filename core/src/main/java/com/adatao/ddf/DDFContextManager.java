package com.adatao.ddf;

import java.util.Iterator;
import java.util.Map;
import java.util.Vector;
import java.util.ServiceLoader;

import com.adatao.ddf.exception.DDFException;

/**
 * A DDF Context Manager help creating correct DDF Context from
 * the specified connection strings.
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 *
 */
public class DDFContextManager {
    private static Vector<DDFContextFactoryInfo> contextFactoryList = new Vector<DDFContextFactoryInfo>();
    
    public static DDFContextFactory getContextFactory(String connectionURL) 
            throws DDFException {
        try {
            Class.forName("com.adatao.ddf.spark.DDFContextFactory");
        } catch (ClassNotFoundException e) {
            System.out.println(e);
        }
        
        for (DDFContextFactoryInfo contextFactoryInfo: contextFactoryList) {
            try {
                if (contextFactoryInfo.contextFactory.acceptURL(connectionURL)) {
                    return contextFactoryInfo.contextFactory;
                }
            } catch (DDFException e) {
                e.printStackTrace();
            }
        }
        
        throw new DDFException("Cannot find any DDF contextFactorys that can handle the connection string: "
                + connectionURL);
    }
    
    /**
     * Get DDFContextFactory directly from DDF ContextFactory Manager.
     * 
     * @param connectionURL
     * @param connectionProps
     * @return
     * @throws DDFException
     */
    public static DDFContext getDDFContext(String connectionURL) 
            throws DDFException {
        DDFContextFactory contextFactory = getContextFactory(connectionURL);
        return contextFactory.connect(connectionURL);
    }
    
    /**
     * Get DDFContextFactory directly from DDF ContextFactory Manager.
     * 
     * @param connectionURL
     * @param connectionProps
     * @return
     * @throws DDFException
     */
    public static DDFContext getDDFContext(String connectionURL, Map<String, String> connectionProps) 
            throws DDFException {
        DDFContextFactory contextFactory = getContextFactory(connectionURL);
        return contextFactory.connect(connectionURL, connectionProps);
    }
    
    
    private static void loadDDFContextFactorys() {
        // use class loader to load all the available DDFContextFactorys into
        // memory.
        ServiceLoader<DDFContextFactory> loadedContextFactories = ServiceLoader.load(DDFContextFactory.class);
        Iterator<DDFContextFactory> contextFactoryIterator = loadedContextFactories.iterator();

        try{
            while(contextFactoryIterator.hasNext()) {
                System.out.println(" Loading done by the java.util.ServiceLoader :  "+contextFactoryIterator.next());
            }
        } catch(Throwable t) {
        // Do nothing
        }
    }
    
    /**
     * Register a DDF contextFactory with the ContextFactoryManager.
     * 
     * @param contextFactory
     */
    public static void registerDDFContextFactory(DDFContextFactory contextFactory) 
            throws DDFException {
        synchronized (DDFContextManager.class) {
            DDFContextFactoryInfo info = new DDFContextFactoryInfo();
            info.contextFactory = contextFactory;
            info.contextFactoryClass = contextFactory.getClass();
            info.contextFactoryClassName = contextFactory.getClass().getName();
                    
            contextFactoryList.add(info);
            
            System.out.println("Finished registering contextFactory: " + info.contextFactoryClassName);
        }
    }
}

class DDFContextFactoryInfo {
    DDFContextFactory contextFactory;
    Class contextFactoryClass;
    String contextFactoryClassName;
}
