package com.adatao.pa.spark.execution;


import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.ddf.DDF;
import io.ddf.etl.IHandleMissingData.Axis;
import io.ddf.etl.IHandleMissingData.NAChecking;
import io.ddf.exception.DDFException;
import com.adatao.pa.AdataoException;
import com.adatao.pa.AdataoException.AdataoExceptionCode;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.Utils;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.IExecutor;


public class DropNA implements IExecutor {
  public static Logger LOG = LoggerFactory.getLogger(DropNA.class);
  private String axis;
  private String how;
  private long thresh;
  private List<String> columns;
  private String dataContainerID;


  public DropNA(String axis, String how, long thresh, List<String> columns, String dataContainerID) {
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

      DDF ddf = sparkThread.getDDFManager().getDDF(dataContainerID);
      DDF newddf = ddf.getMissingDataHandler().dropNA(Axis.fromString(axis), NAChecking.fromString(how), thresh, columns);

      return new Utils.DataFrameResult(newddf);

    } catch (Exception e) {
      throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, e.getMessage(), e);
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
