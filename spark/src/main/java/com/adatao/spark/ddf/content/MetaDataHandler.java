package com.adatao.spark.ddf.content;

import java.util.List;

import org.apache.log4j.Logger;

import com.adatao.ddf.DDF;
import com.adatao.ddf.content.AMetaDataHandler;
import com.adatao.ddf.exception.DDFException;

/**
 * 
 * @author bhan
 * 
 */
public class MetaDataHandler extends AMetaDataHandler {
  private static Logger logger = Logger.getLogger(MetaDataHandler.class);

  public MetaDataHandler(DDF theDDF) {
    super(theDDF);
  }

  @Override
  protected long getNumRowsImpl() {
    String tableName = this.getDDF().getSchemaHandler().getTableName();
    logger.debug("get NumRows Impl called");
    try {
      List<String> rs = this.getManager().sql2txt("SELECT COUNT(*) FROM " + tableName);
      return Long.parseLong(rs.get(0));
    } catch (DDFException e) {
      logger.error("Unable to query from " + tableName, e);
    }
    return 0;
  }

}
