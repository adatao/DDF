/**
 * 
 */
package com.adatao.spark.ddf.content;

import java.util.List;

import shark.api.ColumnDesc;

import com.adatao.ddf.ADDFManager;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.content.SchemaHandler;
import com.adatao.ddf.content.Schema.Column;
import com.google.common.collect.Lists;

/**
 * @author ctn
 * 
 */
public class SparkSchemaHandler extends SchemaHandler {

  public SparkSchemaHandler(ADDFManager theContainer) {
    super(theContainer);
    // TODO Auto-generated constructor stub
  }

  public static Schema getSchemaFrom(ColumnDesc[] sharkColumns) {
    List<Column> cols = Lists.newArrayList();  
    for (ColumnDesc sharkColumn : sharkColumns) {
      cols.add(new Column(sharkColumn.columnName(), sharkColumn.typeName()));
    }
    
    return new Schema(null, cols);
  }
}
