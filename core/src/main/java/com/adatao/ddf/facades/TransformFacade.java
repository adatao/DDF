package com.adatao.ddf.facades;

import com.adatao.ddf.DDF;
import com.adatao.ddf.etl.IHandleTransformations;

public class TransformFacade implements IHandleTransformations {
  private DDF mDDF;
  private IHandleTransformations mTransformationHandler;{

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
}