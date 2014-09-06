package com.adatao.pa.spark.execution;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.exception.DDFException;
import io.ddf.ml.IModel;
import com.adatao.pa.AdataoException;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.execution.CExecutor;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.SuccessResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private IModel mModel;

    public LinearRegResult(IModel model) {
      mModel = model;
    }

    public IModel getModel() {
      return mModel;
    }
  }
  public static Logger LOG = LoggerFactory.getLogger(VectorCorrelation.class);

  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {

    LOG.info(">>>> mDataContainerID = " + mdataContainerID);
    LOG.info(">>>> mIter = " + mIter);
    LOG.info(">>>> mstepSize = " + mstepSize);
    LOG.info(">>>> mminiBatchFraction = " + mminiBatchFraction);

    DDFManager ddfManager = sparkThread.getDDFManager();
    DDF ddf = ddfManager.getDDF(mdataContainerID);
    IModel model;

    try{
      model =  ddf.ML.train("linearRegressionWithSGD", mIter, mstepSize, mminiBatchFraction);
    } catch (DDFException e) {
      throw new AdataoException(AdataoException.AdataoExceptionCode.ERR_GENERAL, e.getMessage(), null);
    }

    return new LinearRegResult(model);
  }
}
