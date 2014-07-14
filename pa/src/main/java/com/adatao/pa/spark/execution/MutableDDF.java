package com.adatao.pa.spark.execution;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.ddf.DDF;
import com.adatao.pa.AdataoException;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.execution.Sql2DataFrame.Sql2DataFrameResult;
import com.adatao.pa.spark.types.ExecutorResult;

public class MutableDDF extends CExecutor {

  public static Logger LOG = LoggerFactory.getLogger(MutableDDF.class);

  private String dataContainerID;
  private boolean isMutable;


  public MutableDDF(String dataContainerID, boolean isMutable) {
    this.dataContainerID = dataContainerID;
    this.isMutable = isMutable;
  }

  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
    DDF ddf = (DDF) sparkThread.getDDFManager().getDDF(dataContainerID);
    ddf.setMutable(isMutable);

    if (ddf == null) {
      LOG.error("Cannot find the DDF " + dataContainerID);
    } else {
      LOG.info("Found the DDF " + dataContainerID);
    }
    // return null;
    // TODO re-check this return type
    return new Sql2DataFrameResult(ddf);
  }

}
