package com.adatao.ddf;

import com.adatao.ddf.exception.DDFException;

/**
 * A DDFDriver is the initial contact point to the cluster that the user
 * obtains from the DDFDriverManager.
 * This class helps the user obtain DDF in various ways.
 * 
 * The DDFDriver is also a Factory of DDF.
 * 
 * @author Cuong Kien Bui
 * @version 0.1
 *
 */
public interface DDFDriver {
    /**
     * Create an empty DDF with a schema definition.
     * 
     * @param schemaString
     * @return
     * @throws DDFException
     */
    public DDF createSchema(String schemaString) throws DDFException;
    
    /**
     * Create a DDF from an sql command.
     * 
     * @param sqlCommand
     * @return
     * @throws DDFException
     */
    public DDF fromSql(String sqlCommand) throws DDFException;
    
    /**
     * Checks if the driver can accepts the proposed URL.
     *  
     * @param connectionURL
     * @return true if the driver accepts this connection URL.
     * @throws DDFException if there is DDF error occurs.
     */
    public boolean acceptURL(String connectionURL) throws DDFException;
}
