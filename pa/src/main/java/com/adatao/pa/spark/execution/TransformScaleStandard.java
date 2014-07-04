package com.adatao.pa.spark.execution;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.ddf.DDF;
import com.adatao.pa.AdataoException;
import com.adatao.pa.AdataoException.AdataoExceptionCode;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.Utils;
import com.adatao.pa.spark.types.ExecutorResult;

// Create a DDF from an SQL Query
@SuppressWarnings("serial")
public class TransformScaleStandard extends CExecutor {

  private String dataContainerID;
  public static Logger LOG = LoggerFactory.getLogger(TransformScaleStandard.class);


  public TransformScaleStandard(String dataContainerID) {
    this.dataContainerID = dataContainerID;
  }

  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
    try {

      DDF ddf = sparkThread.getDDFManager().getDDF(dataContainerID);
      DDF newddf = ddf.Transform.transformScaleStandard();

      return new Utils.DataFrameResult(newddf);

    } catch (Exception e) {

      if (e instanceof shark.api.QueryExecutionException) {
        throw new AdataoException(AdataoExceptionCode.ERR_LOAD_TABLE_FAILED, e.getMessage(), null);
      } else {
        LOG.error("Cannot transform the DDF", e);
        return null;
      }
    }
  }

  public String getDataContainerID() {
    return dataContainerID;
  }

  public TransformScaleStandard setDataContainerID(String dataContainerID) {
    this.dataContainerID = dataContainerID;
    return this;
  }
}
