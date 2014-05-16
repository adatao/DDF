package com.adatao.pa.spark.execution;


import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.pa.AdataoException;
import com.adatao.pa.AdataoException.AdataoExceptionCode;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.Utils;
import com.adatao.pa.spark.types.ExecutorResult;
import com.google.common.collect.Lists;

public class TransformHive extends CExecutor {

  private String dataContainerID;
  private String transformExpression;
  private List<String> columns;
  public static Logger LOG = LoggerFactory.getLogger(TransformHive.class);


  public void initialize(String dataContainerID, String transformExpression, List<String> columns) {
    this.dataContainerID = dataContainerID;
    this.transformExpression = transformExpression;
    this.columns = columns;
  }

  public TransformHive(String dataContainerID, String transformExpression, List<String> columns) {
    initialize(dataContainerID, transformExpression, columns);
  }
  public TransformHive(String dataContainerID, String transformExpression) {
    initialize(dataContainerID, transformExpression, null);
  }

  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {
    try {
      
      DDFManager manager = sparkThread.getDDFManager();
      DDF ddf = manager.getDDF(("SparkDDF-spark-" + dataContainerID).replace("-", "_"));

      ddf = ddf.Transform.transformUDF(transformExpression);
      
      LOG.info(manager.getDDFs().keySet().toString());

      return new Utils.DataFrameResult(ddf);

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

  public TransformHive setDataContainerID(String dataContainerID) {
    this.dataContainerID = dataContainerID;
    return this;
  }
}
