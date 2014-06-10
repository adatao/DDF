package com.adatao.pa.spark.execution;


import java.util.List;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.adatao.pa.AdataoException;
import com.adatao.pa.AdataoException.AdataoExceptionCode;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.Utils;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.IExecutor;

public class GroupBy implements IExecutor {
  public static Logger LOG = LoggerFactory.getLogger(GroupBy.class);

  private String dataContainerID;
  private List<String> groupedColumns;
  private List<String> selectFunctions;

  private void checkParameters() throws AdataoException {
    Utils.assertNull(dataContainerID, new AdataoException(AdataoExceptionCode.ERR_MISSING_DATAFRAME, null));
    Utils.assertNullorEmpty(groupedColumns, new AdataoException(AdataoExceptionCode.ERR_UNSUPPORTED_GROUP_SYNTAX,
        "Grouped columns list is empty", null));
    Utils.assertNullorEmpty(selectFunctions, new AdataoException(AdataoExceptionCode.ERR_UNSUPPORTED_GROUP_SYNTAX,
        "Aggregated function list is empty", null));
    
    // we only accept selectFunction with the following format
    // wildcast is not supported at the moment because of
    // https://app.asana.com/0/5660773543914/11254907079946
    Pattern colNameRegex = Pattern.compile("^'*\\s*[a-zA-Z_][a-zA-Z0-9_]*\\s*'*$");
    Pattern funcRegex = Pattern.compile("^'*\\s*[a-zA-Z_][a-zA-Z0-9_]*\\s*\\(\\s*[a-zA-Z_][a-zA-Z0-9_]*\\s*\\)\\s*'*$");
    Pattern funcWithColNameRegex = Pattern.compile("^'*\\s*[a-zA-Z_][a-zA-Z0-9_]*\\s*=\\s*[a-zA-Z_][a-zA-Z0-9_]*\\s*\\(\\s*[a-zA-Z_][a-zA-Z0-9_]*\\s*\\)\\s*'*$");
    
    for(String s: selectFunctions) {
      if( !colNameRegex.matcher(s).matches() && !funcWithColNameRegex.matcher(s).matches() && !funcRegex.matcher(s).matches()){
        throw new AdataoException(AdataoExceptionCode.ERR_UNSUPPORTED_GROUP_SYNTAX,
            String.format("Unsupported column name or function: %s", s), null);
      }
    }
  }

  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
    DDF ddf = (DDF) sparkThread.getDDFManager().getDDF(Utils.getDDFNameFromDataContainerID(dataContainerID));

    checkParameters();
    if (ddf == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_DATAFRAME_NONEXISTENT, "Cannot find DDF " + dataContainerID,
          null);
    }

    try {
      DDF res = ddf.groupBy(groupedColumns, selectFunctions);
      return new com.adatao.pa.spark.Utils.DataFrameResult(res);

    } catch (DDFException e) {
      throw new AdataoException(AdataoExceptionCode.ERR_SHARK_QUERY_FAILED, e.getMessage(), null);
    }
  }

}
