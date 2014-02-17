package com.adatao.spark.ddf.content;

import java.util.List;

import org.apache.log4j.Logger;

import com.adatao.ddf.ADDFManager;
import com.adatao.ddf.content.AMetaDataHandler;
import com.adatao.ddf.exception.DDFException;
/**
 * 
 * @author bhan
 *
 */
public class MetaDataHandler extends AMetaDataHandler {
  private static Logger logger = Logger.getLogger(MetaDataHandler.class);

  public MetaDataHandler(ADDFManager theDDFManager) {
    super(theDDFManager);
  }

  @Override
  protected long getNumRowsImpl() {
    String tableName = this.getManager().getSchemaHandler().getTableName();
    try {
      List<String> rs = this.getManager().getDataCommandHandler()
          .cmd2txt("select count(*) from " + tableName);
      return Long.parseLong(rs.get(0));
    } catch (DDFException e) {
      logger.error("Unable to query from " + tableName, e);
    }
    return 0;
  }

}
