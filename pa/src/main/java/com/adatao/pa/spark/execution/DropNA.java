package com.adatao.pa.spark.execution;


import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.ddf.DDF;
import com.adatao.ddf.etl.IHandleMissingData.Axis;
import com.adatao.ddf.etl.IHandleMissingData.NAChecking;
import com.adatao.ddf.exception.DDFException;
import com.adatao.pa.AdataoException;
import com.adatao.pa.AdataoException.AdataoExceptionCode;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.Utils;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.IExecutor;


public class DropNA implements IExecutor {
  public static Logger LOG = LoggerFactory.getLogger(DropNA.class);
  private Axis axis;
  private NAChecking how;
  private long thresh;
  private List<String> columns;
  private String dataContainerID;


  public DropNA(Axis axis, NAChecking how, long thresh, List<String> columns, String dataContainerID) {
    this.axis = axis;
    this.how = how;
    this.thresh = thresh;
    this.columns = columns;
    this.dataContainerID = dataContainerID;
  }

  // public DDF dropNA(Axis axis, NAChecking how, long thresh, List<String> columns) throws DDFException;
  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
    try {

      DDF ddf = sparkThread.getDDFManager().getDDF(("SparkDDF-spark-" + dataContainerID).replace("-", "_"));
      DDF newddf = ddf.getMissingDataHandler().dropNA(axis, how, thresh, columns);

      return new Utils.DataFrameResult(newddf);

    } catch (Exception e) {

      if (e instanceof shark.api.QueryExecutionException) {
        throw new AdataoException(AdataoExceptionCode.ERR_LOAD_TABLE_FAILED, e.getMessage(), null);
      } else {
        LOG.error("Cannot filter out NAs from the DDF", e);
        return null;
      }
    }
  }

  public String getDataContainerID() {
    return dataContainerID;
  }

  public DropNA setDataContainerID(String dataContainerID) {
    this.dataContainerID = dataContainerID;
    return this;
  }

}
