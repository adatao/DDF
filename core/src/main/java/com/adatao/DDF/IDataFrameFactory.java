package com.adatao.DDF;

public interface IDataFrameFactory {
  public DataFrame newInstance();
  
  public void setBasicStatisticComputer(IComputeBasicStatistics x);
  public void setFilteringAndProjectionHandler(IHandleFilteringAndProjections x);
  public void setIndexingHandler(IHandleIndexing x);
  public void setJoinsHandler(IHandleJoins x);
  public void setIMiscellanyHandler(IHandleMiscellany x);
  public void setIMissingDataHandler(IHandleMissingData x);
  public void setIMutabilityHandler(IHandleMutability x);
  public void setPersistenceHandler(IHandlePersistence x);
  public void setReshapingHandler(IHandleReshaping x);
  public void setBasicStatisticComputer(IComputeBasicStatistics x);
}
