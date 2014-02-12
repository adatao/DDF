package com.adatao.ddf;

import com.adatao.ddf.exception.DDFException;

public interface DDFContext {
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
     * read a CSV file from HDFS file system or local file system and return
     * a DDF which represent it.
     * 
     * This method will read the CSV and try to guess the table schema out of it.
     * 
     * @return the DDF represents the CSV data.
     */
    public DDF readCSV(String csvFile) throws DDFException;

}
