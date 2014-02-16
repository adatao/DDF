package com.adatao.spark.ddf.content;

import java.util.List;

import org.apache.log4j.Logger;

import com.adatao.ddf.ADDFManager;
import com.adatao.ddf.content.AMetaDataHandler;
import com.adatao.ddf.exception.DDFException;

public class SparkMetaDataHandler extends AMetaDataHandler {
  private static Logger logger = Logger.getLogger(SparkMetaDataHandler.class);

  public SparkMetaDataHandler(ADDFManager theContainer) {
    super(theContainer);
  }

  @Override
  protected long getNumRowsImpl() {
    String tableName = this.getContainer().getSchemaHandler().getTableName();
    try {
      List<String> rs = this.getContainer().getDataCommandHandler()
          .cmd2txt("select count(*) from " + tableName);
      return Long.parseLong(rs.get(0));
    } catch (DDFException e) {
      logger.error("Unable to query from " + tableName, e);
    }
    return 0;
  }

}
