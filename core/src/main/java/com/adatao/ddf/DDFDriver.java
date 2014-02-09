package com.adatao.ddf;

import com.adatao.ddf.exception.DDFException;

/**
 * A DDFDriver is the initial contact point to the cluster that the user
 * obtains from the DDFDriverManager.
 * This class helps the user obtain DDF in various ways.
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
    public DDF create_schema(String schemaString) throws DDFException;
    
    /**
     * Create a DDF from an sql command.
     * 
     * @param sqlCommand
     * @return
     * @throws DDFException
     */
    public DDF fromSql(String sqlCommand) throws DDFException;
}
