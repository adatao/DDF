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
    private static Vector<DDFContextInfo> contextList = new Vector<DDFContextInfo>();
    
    public static DDFContext getContext(String connectionURL) 
            throws DDFException {
        try {
            Class.forName("com.adatao.ddf.spark.DDFContext");
        } catch (ClassNotFoundException e) {
            System.out.println(e);
        }
        
        for (DDFContextInfo contextInfo: contextList) {
            try {
                if (contextInfo.context.acceptURL(connectionURL)) {
                    return contextInfo.context;
                }
            } catch (DDFException e) {
                e.printStackTrace();
            }
        }
        
        throw new DDFException("Cannot find any DDF contexts that can handle the connection string: "
                + connectionURL);
    }
    
    /**
     * Get DDFFactory directly from DDF Context Manager.
     * 
     * @param connectionURL
     * @param connectionProps
     * @return
     * @throws DDFException
     */
    public static DDFFactory getDDFFactory(String connectionURL, Map<String, String> connectionProps) 
            throws DDFException {
        DDFContext context = getContext(connectionURL);
        return context.connect(connectionURL, connectionProps);
    }
    
    
    private static void loadDDFContexts() {
        // use class loader to load all the available DDFContexts into
        // memory.
        ServiceLoader<DDFContext> loadedContexts = ServiceLoader.load(DDFContext.class);
        Iterator<DDFContext> contextsIterator = loadedContexts.iterator();

        try{
            while(contextsIterator.hasNext()) {
                System.out.println(" Loading done by the java.util.ServiceLoader :  "+contextsIterator.next());
            }
        } catch(Throwable t) {
        // Do nothing
        }
    }
    
    /**
     * Register a DDF context with the ContextManager.
     * 
     * @param context
     */
    public static void registerDDFContext(DDFContext context) 
            throws DDFException {
        synchronized (DDFContextManager.class) {
            DDFContextInfo info = new DDFContextInfo();
            info.context = context;
            info.contextClass = context.getClass();
            info.contextClassName = context.getClass().getName();
                    
            contextList.add(info);
            
            System.out.println("Finished registering context: " + info.contextClassName);
        }
    }
}

class DDFContextInfo {
    DDFContext context;
    Class contextClass;
    String contextClassName;
}