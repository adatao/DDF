package com.adatao.spark.ddf.content;


import java.util.List;
import org.apache.log4j.Logger;
import com.adatao.ddf.DDF;
import com.adatao.ddf.content.AMetaDataHandler;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.exception.DDFException.DDFExceptionCode;

/**
 *
 */
public class MetaDataHandler extends AMetaDataHandler {
  private static Logger logger = Logger.getLogger(MetaDataHandler.class);


  public MetaDataHandler(DDF theDDF) {
    super(theDDF);
  }

  @Override
  protected long getNumRowsImpl() throws DDFException {
    String tableName = this.getDDF().getSchemaHandler().getTableName();
    logger.debug("get NumRows Impl called");

    List<String> rs = this.getManager().sql2txt("SELECT COUNT(*) FROM " + tableName);
    if (rs == null || rs.size() == 0) throw new DDFException(DDFExceptionCode.ERR_SQL_RESULT_EMPTY);
    try {
      return Long.parseLong(rs.get(0));
    } catch (NumberFormatException e){
      throw new DDFException("Cannot convert to number: " + rs.get(0));
    }

  }

}
