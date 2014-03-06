package com.adatao.ddf.facades;


import java.util.Iterator;
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
  public <T> Iterator<T> getRowIterator(Class<T> rowType) {
    return mViewHandler.getRowIterator(rowType);
  }

  @Override
  public Iterator<?> getRowIterator() {
    return mViewHandler.getRowIterator();
  }

  @Override
  public <R, C> Iterator<C> getElementIterator(Class<R> rowType, Class<C> columnType, int columnIndex) {
    return mViewHandler.getElementIterator(rowType, columnType, columnIndex);
  }

  @Override
  public Iterator<?> getElementIterator(int columnIndex) {
    return mViewHandler.getElementIterator(columnIndex);
  }

  @Override
  public <R, C> Iterator<C> getElementIterator(Class<R> rowType, Class<C> columnType, String columnName) {
    return mViewHandler.getElementIterator(rowType, columnType, columnName);
  }

  @Override
  public Iterator<?> getElementIterator(String columnName) {
    return mViewHandler.getElementIterator(columnName);
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
  public DDF firstNRows(int numRows) throws DDFException {
    return mViewHandler.firstNRows(numRows);
  }

  public List<Object[]> getRandomSample(int numSamples) {
    return getRandomSample(numSamples, false, 1);
  }

  @Override
  public DDF project(String[] columnNames) throws DDFException {
    return this.getViewHandler().project(columnNames);
  }

  @Override
  public List<String> sql2txt(String sqlCommand, String errorMessage) throws DDFException {
    return this.getViewHandler().sql2txt(sqlCommand, errorMessage);
  }
}
