package com.adatao.pa.spark.execution;


import com.adatao.ddf.DDF;
import com.adatao.pa.AdataoException;
import com.adatao.pa.spark.types.ExecutionResult;
import com.adatao.pa.spark.types.FailedResult;
import com.adatao.pa.spark.types.SuccessfulResult;

/**
 */

public class DDFExecutor {

  private String methodName;

  private Object[] params;

  private String ddfUri;


  public DDFExecutor(String methodName, String ddfUri, Object... params) {
    this.methodName = methodName;
    this.params = params;
    this.ddfUri = ddfUri;
  }

  public ExecutionResult run(ExecutionContext context) {
    DDF ddf = context.sparkThread().getDDFManager().getDDF(("SparkDDF-spark-" + this.ddfUri).replace("-", "_"));
    try {
      if (ddf == null) {
        throw new AdataoException(AdataoException.AdataoExceptionCode.ERR_DATAFRAME_NONEXISTENT, "Cannot find DDF "
            + this.ddfUri, null);
      }
      return new SuccessfulResult(ddf.PA.runMethod(this.methodName, this.params));

    } catch (Exception e) {
      return new FailedResult(e.getMessage());
    }
  }
}
