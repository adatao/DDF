package com.adatao.ddf.facades;

import java.util.Iterator;
import com.adatao.ddf.DDF;
import com.adatao.ddf.content.IHandleViews;
import com.adatao.ddf.exception.DDFException;

public class ViewsFacade implements IHandleViews{
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

  public void setMLSupporter(IHandleViews viewHandler) {
    mViewHandler = viewHandler;
  }

  @Override
  public <T> Iterator<T> getRowIterator(Class<T> rowType) {
    return mViewHandler.getRowIterator(rowType);
  }

  @Override
  public Iterator<?> getRowIterator() {
    return mViewHandler.getRowIterator();
  }

  @Override
  public <C> Iterator<C> getElementIterator(Class<?> dataType, Class<C> columnType, int columnIndex) {
    return mViewHandler.getElementIterator(dataType, columnType, columnIndex);
  }



  @Override
  public Iterator<?> getElementIterator(int columnIndex) {
    return mViewHandler.getElementIterator(columnIndex);
  }



  @Override
  public <C> Iterator<C> getElementIterator(Class<?> dataType, Class<C> columnType, String columnName) {
    return mViewHandler.getElementIterator(dataType, columnType, columnName);
  }



  @Override
  public Iterator<?> getElementIterator(String columnName) {
    return mViewHandler.getElementIterator(columnName);
  }



  @Override
  public DDF getRandomSample(int numSamples, boolean withReplacement, int seed) {
    return mViewHandler.getRandomSample(numSamples, withReplacement, seed);
  }



  @Override
  public DDF getRandomSample(double percent, boolean withReplacement, int seed) {
    return mViewHandler.getRandomSample(percent, withReplacement, seed);
  }


  @Override
  public DDF firstNRows(int numRows) throws DDFException{
    return mViewHandler.firstNRows(numRows);
  }
  
  public DDF getRandomSample(int numSamples) {
    return getRandomSample(numSamples, false, 1);
  }
  
  @Override
  public DDF project(String[] columnNames) throws DDFException {
    return this.getViewHandler().project(columnNames);
  }
}
