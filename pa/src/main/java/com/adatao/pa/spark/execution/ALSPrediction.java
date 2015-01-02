package com.adatao.pa.spark.execution;


import com.adatao.ML.spark.recommendation.ALSModel;
import com.adatao.pa.AdataoException;
import com.adatao.pa.AdataoException.AdataoExceptionCode;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.IExecutor;
import com.adatao.pa.spark.types.SuccessResult;
import io.ddf.ml.IModel;

public class ALSPrediction implements IExecutor {
  String modelId;
  int userId;
  int[] productIds;


  public ALSPrediction(String modelId, int userId, int[] productIds) {
    super();
    this.modelId = modelId;
    this.userId = userId;
    this.productIds = productIds;
  }


  static public class ALSPredictionResult extends SuccessResult {
    public double[] scores;

    public ALSPredictionResult(double[] scores) {
      this.scores = scores;
    }
  }


  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {

    IModel model = sparkThread.getDDFManager().getModel(modelId);

    if (model == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, "model doesn't exist", null);
    }

    ALSModel alsmodel = (ALSModel) model.getRawModel();
    try {
      double[] scores = alsmodel.predict(userId, productIds);
      return new ALSPredictionResult(scores);
    } catch (Exception e) {
      throw new AdataoException(AdataoExceptionCode.ERR_GENERAL, "error predicting", null);
    }

  }

}
