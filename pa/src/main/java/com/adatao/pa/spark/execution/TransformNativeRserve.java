package com.adatao.pa.spark.execution;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.ddf.DDF;
import io.ddf.DDFManager;
import com.adatao.pa.AdataoException;
import com.adatao.pa.AdataoException.AdataoExceptionCode;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.Utils;
import com.adatao.pa.spark.types.ExecutorResult;

@SuppressWarnings("serial")
public class TransformNativeRserve extends CExecutor {

  private String dataContainerID;
  private String transformExpression;
  public static Logger LOG = LoggerFactory.getLogger(TransformNativeRserve.class);


  public TransformNativeRserve(String dataContainerID, String transformExpression) {
    this.dataContainerID = dataContainerID;
    this.transformExpression = transformExpression;
  }

  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
    try {

      DDFManager manager = sparkThread.getDDFManager();
      DDF ddf = manager.getDDF(dataContainerID);
      DDF newddf = ddf.Transform.transformNativeRserve(transformExpression);
      LOG.info("Transformed DDF name " + newddf.getName());
      manager.addDDF(newddf);
      LOG.info(manager.getDDFs().keySet().toString());

      return new Utils.DataFrameResult(newddf);

    } catch (Exception e) {
      throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, e.getMessage(), e);
    }
  }

  public String getDataContainerID() {
    return dataContainerID;
  }

  public TransformNativeRserve setDataContainerID(String dataContainerID) {
    this.dataContainerID = dataContainerID;
    return this;
  }
}
