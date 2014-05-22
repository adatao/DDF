package com.adatao.pa.spark.execution;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.ddf.DDF;
import com.adatao.pa.AdataoException;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.Utils;
import com.adatao.pa.spark.execution.NRow.NRowResult;
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

    DDF ddf = (DDF) sparkThread.getDDFManager().getDDF(("SparkDDF-spark-" + dataContainerID).replace("-", "_"));
    
    ddf.setMutable(isMutable);

    if (ddf == null) {
      LOG.info("Cannot find the DDF " + dataContainerID);
    } else {
      LOG.info("Found the DDF " + dataContainerID);
    }
    return null;
  }

}
