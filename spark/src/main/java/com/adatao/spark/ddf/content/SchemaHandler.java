/**
 * 
 */
package com.adatao.spark.ddf.content;

import java.util.*;

import com.adatao.ddf.Factor;
import com.adatao.ddf.content.IHandleRepresentations;
import com.adatao.ddf.exception.DDFException;
import com.adatao.spark.ddf.SparkDDF;
import org.apache.spark.rdd.RDD;
import scala.reflect.ClassManifest$;
import shark.api.ColumnDesc;

import com.adatao.ddf.DDF;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.content.Schema.Column;
import com.google.common.collect.Lists;
import shark.memstore2.TablePartition;

/**
 * @author ctn
 * 
 */
public class SchemaHandler extends com.adatao.ddf.content.SchemaHandler {

  public SchemaHandler(DDF theDDF) {
    super(theDDF);
  }

  public static Schema getSchemaFrom(ColumnDesc[] sharkColumns) {
    List<Column> cols = Lists.newArrayList();
    for (ColumnDesc sharkColumn : sharkColumns) {
      cols.add(new Column(sharkColumn.columnName(), sharkColumn.typeName()));
    }

    return new Schema(null, cols);
  }

  @Override
  public void getFactorLevelsAndLevelCounts() throws DDFException {
    List<Integer> columnIndexes = new ArrayList<Integer>();
    List<Schema.ColumnType> columnTypes = new ArrayList<Schema.ColumnType>();

    for(Column col : this.getColumns()) {
      if(col.getColumnClass() == Schema.ColumnClass.FACTOR) {
        Factor<?> colFactor = col.getOptionalFactor();

        if(colFactor == null || colFactor.getLevelCounts() == null || colFactor.getLevels() == null){
          if(colFactor == null) {
            colFactor = this.setAsFactor(col.getName());
          }
          columnIndexes.add(this.getColumnIndex(col.getName()));
          columnTypes.add(col.getType());
          //factors.add(colFactor);
        }
      }
    }

    Map<Integer, Map<String, Integer>> listLevelCounts;

    IHandleRepresentations repHandler = this.getDDF().getRepresentationHandler();
    if(repHandler.has(RDD.class, TablePartition.class)) {
      RDD<TablePartition> rdd =((SparkDDF) this.getDDF()).getRDD(TablePartition.class);
      listLevelCounts = GetMultiFactor.getFactorCounts(rdd,
          columnIndexes, columnTypes, TablePartition.class);
    } else if(repHandler.has(RDD.class, Object[].class)) {
      RDD<Object[]> rdd =((SparkDDF) this.getDDF()).getRDD(Object[].class);
      listLevelCounts = GetMultiFactor.getFactorCounts(rdd,
          columnIndexes, columnTypes, Object[].class);
    } else {
      RDD<Object[]> rdd = ((SparkDDF) this.getDDF()).getRDD(Object[].class);
      if(rdd == null) {
        throw new DDFException("Error setting factor levels counts");
      }
      listLevelCounts = GetMultiFactor.getFactorCounts(rdd, columnIndexes, columnTypes, Object[].class);
    }
    if(listLevelCounts == null) {
      throw new DDFException("Error getting factor levels counts");
    }

    for(Integer colIndex: columnIndexes) {
      Column column = this.getColumn(this.getColumnName(colIndex));
      Map<String, Integer> levelCounts = listLevelCounts.get(colIndex);
      Factor<?> factor = column.getOptionalFactor();

      List<String> levels = new ArrayList<String>(levelCounts.keySet());

      factor.setLevelCounts(levelCounts);
      factor.setLevels(levels, false);
    }
  }
}
