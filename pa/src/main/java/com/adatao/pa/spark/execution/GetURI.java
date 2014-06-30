package com.adatao.pa.spark.execution;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.ddf.DDF;
import com.adatao.pa.AdataoException;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.SuccessResult;

@SuppressWarnings("serial")
public class GetURI extends CExecutor {
  private String dataContainerID;

  public static Logger LOG = LoggerFactory.getLogger(GetURI.class);

  static public class StringResult extends SuccessResult {
    public String str;


    public StringResult(String str) {
      this.str = str;
    }
  }

  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {

    DDF ddf = (DDF) sparkThread.getDDFManager().getDDF(dataContainerID);
    if (ddf == null) {
      LOG.error("Cannot find the DDF " + dataContainerID);
    } else {
      LOG.info("Found the DDF " + dataContainerID);
    }
    return new StringResult(ddf.getUri());
  }

  public GetURI setDataContainerID(String dataContainerID) {
    this.dataContainerID = dataContainerID;
    return this;
  }

}
