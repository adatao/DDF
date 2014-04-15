package com.adatao.ddf.facades;

import com.adatao.ddf.DDF;
import com.adatao.ddf.etl.IHandleTransformations;
import com.adatao.ddf.exception.DDFException;

public class TransformFacade implements IHandleTransformations {
  private DDF mDDF;
  private IHandleTransformations mTransformationHandler;{

}

  public TransformFacade(DDF ddf, IHandleTransformations transformationHandler) {
    this.mDDF = ddf;
    this.mTransformationHandler = transformationHandler;
  }

  @Override
  public DDF getDDF() {
    return mDDF;
  }

  @Override
  public void setDDF(DDF theDDF) {
    mDDF = theDDF;
  }
  
  public IHandleTransformations getmTransformationHandler() {
    return mTransformationHandler;
  }

  public void setmTransformationHandler(IHandleTransformations mTransformationHandler) {
    this.mTransformationHandler = mTransformationHandler;
  }

  @Override
  public DDF transformNativeRserve(String transformExpression) {
    return mTransformationHandler.transformNativeRserve(transformExpression);
    
  }

  @Override
  public DDF transformScaleMinMax() throws DDFException {
    return mTransformationHandler.transformScaleMinMax();
  }

  @Override
  public DDF transformScaleStandard() throws DDFException {
    return mTransformationHandler.transformScaleStandard();
  }
  
}