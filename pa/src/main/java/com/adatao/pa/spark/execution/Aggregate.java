package com.adatao.pa.spark.execution;


import io.ddf.DDF;
import io.ddf.types.AggregateTypes.AggregationResult;
import io.ddf.exception.DDFException;
import com.adatao.pa.AdataoException;
import com.adatao.pa.AdataoException.AdataoExceptionCode;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.Utils;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.IExecutor;
import com.adatao.pa.spark.types.SuccessResult;

public class Aggregate implements IExecutor {
  private String dataContainerID;
  private String[] colnames;
  private String[] groupBy;
  private String func;


  public Aggregate(String dataContainerID, String[] colnames, String[] groupBy, String func) {
    this.dataContainerID = dataContainerID;
    this.colnames = colnames;
    this.groupBy = groupBy;
    this.func = func;
  }


  static public class AggregateResult extends SuccessResult {
    AggregationResult results;


    public AggregateResult setResults(AggregationResult results) {
      this.results = results;
      return this;
    }

    public AggregationResult getResults() {
      return results;
    }
  }


  @Override
  public ExecutorResult run(SparkThread sparkThread) throws AdataoException {

    DDF ddf = (DDF) sparkThread.getDDFManager().getDDF(dataContainerID);

    if (ddf == null) {
      throw new AdataoException(AdataoExceptionCode.ERR_DATAFRAME_NONEXISTENT, "Cannot find DDF " + dataContainerID,
          null);
    }

    try {
      AggregationResult res = ddf.aggregate(createAggregationFormula(colnames, groupBy, func));
      return new AggregateResult().setResults(res);
    } catch (DDFException e) {
      throw new AdataoException(AdataoExceptionCode.ERR_SHARK_QUERY_FAILED, e.getMessage(), null);
    }

  }

  private String createAggregationFormula(String[] colnames, String[] groupBy, String func) {

    StringBuffer fieldSpecs = new StringBuffer("");
    for (String col : groupBy) {
      fieldSpecs.append(String.format("%s,", col));
      fieldSpecs.append(",");
    }

    for (String col : colnames) {
      fieldSpecs.append(String.format("%s(%s),", func, col));
    }
    if (fieldSpecs.length() > 0) {
      fieldSpecs.setLength(fieldSpecs.length() - 1);
    }
    return fieldSpecs.toString();
  }

}
