package com.adatao.spark.ddf.content;

import com.adatao.ddf.content.AMetaDataHandler;
import com.adatao.spark.ddf.SparkDDFManager;

public class SparkMetaDataHandler extends AMetaDataHandler {
  
  public SparkMetaDataHandler(SparkDDFManager ddfManager) {
    super(ddfManager);
  }

  @Override
  protected long getNumRowsImpl() {
    // TODO Auto-generated method stub
    return 0;
  }
}
