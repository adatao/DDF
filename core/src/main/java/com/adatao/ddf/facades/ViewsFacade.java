package com.adatao.ddf.facades;


import java.util.List;
import com.adatao.ddf.DDF;
import com.adatao.ddf.content.IHandleViews;
import com.adatao.ddf.exception.DDFException;

public class ViewsFacade implements IHandleViews {
  private DDF mDDF;
  private IHandleViews mViewHandler;


  public ViewsFacade(DDF ddf, IHandleViews mlSupporter) {
    mDDF = ddf;
    mViewHandler = mlSupporter;
  }

  @Override
  public DDF getDDF() {
    return mDDF;
  }

  @Override
  public void setDDF(DDF theDDF) {
    mDDF = theDDF;
  }

  public IHandleViews getViewHandler() {
    return mViewHandler;
  }

  public void setViewHandler(IHandleViews viewHandler) {
    mViewHandler = viewHandler;
  }

  @Override
  public List<Object[]> getRandomSample(int numSamples, boolean withReplacement, int seed) {
    return mViewHandler.getRandomSample(numSamples, withReplacement, seed);
  }

  @Override
  public DDF getRandomSample(double percent, boolean withReplacement, int seed) {
    return mViewHandler.getRandomSample(percent, withReplacement, seed);
  }

  @Override
  public List<String> firstNRows(int numRows) throws DDFException {
    return mViewHandler.firstNRows(numRows);
  }

  public List<Object[]> getRandomSample(int numSamples) {
    return getRandomSample(numSamples, false, 1);
  }

  @Override
  public DDF project(String... columnNames) throws DDFException {
    return this.getViewHandler().project(columnNames);
  }
}
