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

public class TransformHive extends CExecutor {

  private String dataContainerID;
  private String transformExpression;
  public static Logger LOG = LoggerFactory.getLogger(TransformHive.class);


  public TransformHive(String dataContainerID, String transformExpression) {
    this.dataContainerID = dataContainerID;
    this.transformExpression = transformExpression;
  }

  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
    try {
      DDFManager manager = sparkThread.getDDFManager();
      DDF ddf = manager.getDDF(dataContainerID);
      DDF newddf = ddf.Transform.transformUDF(transformExpression);

      if(newddf == null) {
          throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, "Error transform DDF", null);
      } else {
        manager.addDDF(newddf);
        return new Utils.DataFrameResult(newddf);
      }
    } catch (Exception e) {
      throw new AdataoException(AdataoExceptionCode.ERR_GENERAL,"Error transform DDF", e);
    }
  }

  public String getDataContainerID() {
    return dataContainerID;
  }

  public TransformHive setDataContainerID(String dataContainerID) {
    this.dataContainerID = dataContainerID;
    return this;
  }
}
