package com.adatao.spark.ddf.content;


import com.adatao.ddf.ADDFManager;
import com.adatao.ddf.content.ARepresentationHandler;

public class SparkRepresentationHandler extends ARepresentationHandler {

  public SparkRepresentationHandler(ADDFManager container) {
    super(container);
    // TODO Auto-generated constructor stub
  }

  @Override
  public void cacheAll() {
    // TODO Auto-generated method stub

  }

  @Override
  public void uncacheAll() {
    // TODO Auto-generated method stub

  }

  // static class Row2TablePartitionMapper extends Function<Row, TablePartition> {
  // public Row2TablePartitionMapper() {
  // super();
  // }
  //
  // @Override
  // public TablePartition call(Row t) throws Exception {
  // return (TablePartition) t.rawdata();
  // }
  // }

}
