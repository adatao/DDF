package com.adatao.ddf.misc;

import java.util.List;
import org.apache.commons.lang.StringUtils;
import com.adatao.ddf.DDF;
import com.adatao.ddf.analytics.AggregationHandler.AggregateField;
import com.adatao.ddf.analytics.AggregationHandler.AggregationResult;
import com.adatao.ddf.analytics.IHandleAggregation;
import com.adatao.ddf.exception.DDFException;
import com.google.common.base.Joiner;

public class RSupporter implements IHandleAggregation{
  
  private DDF mDDF;
  private IHandleAggregation mAggregationHandler;

  public RSupporter(DDF ddf, IHandleAggregation mlSupporter) {
    mDDF = ddf;
    mAggregationHandler = mlSupporter;
  }

  public IHandleAggregation getAggregationHandler() {
    return mAggregationHandler;
  }

  public void setAggregationHandler(IHandleAggregation AggregationHandler) {
    mAggregationHandler = AggregationHandler;
  }
  
  @Override
  public DDF getDDF() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setDDF(DDF theDDF) {
    // TODO Auto-generated method stub
    
  }
  

  @Override
  public double computeCorrelation(String columnA, String columnB) throws DDFException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public AggregationResult aggregate(List<AggregateField> fields) throws DDFException {
    // TODO Auto-generated method stub
    return null;
  }

/////// Aggregate operations
 //aggregate(cbind(mpg,hp) ~ vs + am, mtcars, FUN=mean)
 public AggregationResult aggregate(String rAggregateFormula) throws DDFException {
   String aggregatedField = rAggregateFormula.split(",")[0].split("~")[0];
   String unAggregatedFields = rAggregateFormula.split(",")[0].split("~")[1].replaceAll("+",",");
   String function = rAggregateFormula.split(",")[2].split("=")[1];
   String fields = Joiner.on(",").join(new AggregateField(aggregatedField, function).toString(),unAggregatedFields);
   return mAggregationHandler.aggregate(AggregateField.fromSqlFieldSpecs(fields));
 } 

}
