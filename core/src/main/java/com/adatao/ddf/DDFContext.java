package com.adatao.ddf;

import java.util.Map;

import com.adatao.ddf.exception.DDFException;

/**
 * A DDFContext is the initial contact point to the cluster that the user
 * obtains from the DDFContextManager.
 * This class helps the user obtain DDF in various ways.
 * 
 * The DDFContext is also a Factory of DDF.
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 *
 */
public interface DDFContext {
    /**
     * Checks if the driver can accepts the proposed URL.
     *  
     * @param connectionURL
     * @return true if the driver accepts this connection URL.
     * @throws DDFException if there is DDF error occurs.
     */
    public boolean acceptURL(String connectionURL) throws DDFException;
    
    /**
     * Connect to the cluster and return a DDFFactory.
     * 
     * @param connectionURL
     * @param connectionProps
     * @return
     * @throws DDFException
     */
    public DDFFactory connect(String connectionURL, Map<String, String> connectionProps) throws DDFException;
}
