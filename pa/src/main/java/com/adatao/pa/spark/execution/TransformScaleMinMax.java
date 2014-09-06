package com.adatao.pa.spark.execution;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.ddf.DDF;
import com.adatao.pa.AdataoException;
import com.adatao.pa.AdataoException.AdataoExceptionCode;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.Utils;
import com.adatao.pa.spark.types.ExecutorResult;

// Create a DDF from an SQL Query
@SuppressWarnings("serial")
public class TransformScaleMinMax extends CExecutor {

  private String dataContainerID;
  public static Logger LOG = LoggerFactory.getLogger(TransformScaleMinMax.class);


  public TransformScaleMinMax(String dataContainerID) {
    this.dataContainerID = dataContainerID;
  }

  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
    try {

      DDF ddf = sparkThread.getDDFManager().getDDF(dataContainerID);
      DDF newddf = ddf.Transform.transformScaleMinMax();

      return new Utils.DataFrameResult(newddf);

    } catch (Exception e) {
      LOG.error("Cannot transform the DDF", e);
      return null;
    }
  }

  public String getDataContainerID() {
    return dataContainerID;
  }

  public TransformScaleMinMax setDataContainerID(String dataContainerID) {
    this.dataContainerID = dataContainerID;
    return this;
  }
}
