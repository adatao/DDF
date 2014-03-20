package com.adatao.ML;

import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.ml.IModel;
import com.adatao.pa.AdataoException;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.execution.CExecutor;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.SuccessResult;

/**
 * author: daoduchuan
 */
public class LinearReg extends CExecutor {
  private String mdataContainerID;
  private Integer mIter;
  private Double mstepSize;
  private Double mminiBatchFraction;

  public LinearReg(String dataContainerID, Integer iter, Double stepSize, Double miniBatachFraction) {
    mdataContainerID = dataContainerID;
    mIter = iter;
    mstepSize= stepSize;
    mminiBatchFraction = miniBatachFraction;
  }

  public static class LinearRegResult extends SuccessResult {

  }

  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {

    DDFManager ddfManager = sparkThread.getDDFManager();
    DDF ddf = ddfManager.getDDF(("SparkDDF-spark-" + mdataContainerID).replace("-", "_"));
    IModel model;
    try{
      model =  ddf.ML.train("linearRegressionWithSGD", mIter, mstepSize, mminiBatchFraction);
    } catch (DDFException e) {
      throw new AdataoException(AdataoException.AdataoExceptionCode.ERR_GENERAL, e.getMessage(), null);
    }

  }
}
