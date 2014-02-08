package com.adatao.ddf.content;

import java.util.UUID;

public interface IHandleMetadata {

  public UUID getId();

  public void setId(UUID id);

  // Schema

  public long getNumRows();

  public long getNumColumns();
  
  public ColumnInfo[] getColumnMetadata();
}
