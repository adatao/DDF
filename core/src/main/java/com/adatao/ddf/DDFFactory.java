package com.adatao.ddf;

import com.adatao.ddf.exception.DDFException;

public interface DDFFactory {
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
}
