/**
 * 
 */
package com.adatao.ddf.content;

import java.util.UUID;

import com.adatao.ddf.ADDFFunctionalGroupHandler;
import com.adatao.ddf.ADDFHelper;

/**
 * @author ctn
 * 
 */
public abstract class AMetaDataHandler extends ADDFFunctionalGroupHandler implements IHandleMetadata {

  public AMetaDataHandler(ADDFHelper theContainer) {
    super(theContainer);
  }


  private UUID mId = UUID.randomUUID();

  @Override
  public UUID getId() {
    return mId;
  }

  @Override
  public void setId(UUID id) {
    mId = id;
  }


  private long mNumRows = 0L;
  private boolean bNumRowsIsValid = false;
  private long mNumColumns = 0L;
  private boolean bNumColumnsIsValid = false;
  private ColumnInfo[] columnMetadata;
  
  protected abstract long getNumRowsImpl();

  protected long getNumColumnsImpl() {
    return columnMetadata.length;
  }

  /**
   * Called to assert that the row count needs to be recomputed at next access
   */
  protected void invalidateNumRows() {
    bNumRowsIsValid = false;
  }

  /**
   * Called to assert that the row count needs to be recomputed at next access
   */
  protected void invalidateNumColumns() {
    bNumColumnsIsValid = false;
  }

  @Override
  public long getNumRows() {
    if (!bNumRowsIsValid) {
      mNumRows = this.getNumRowsImpl();
      bNumRowsIsValid = true;
    }
    return mNumRows;
  }

  @Override
  public long getNumColumns() {
    if (!bNumColumnsIsValid) {
      mNumColumns = this.getNumRowsImpl();
      bNumColumnsIsValid = true;
    }
    return mNumColumns;
  }
  public ColumnInfo[] getColumnMetadata() {
    return columnMetadata;
  }
  
  public void setColumnHeaders(String[] headers) {
    int length = headers.length < columnMetadata.length ? headers.length : columnMetadata.length;
    for (int i = 0; i < length; i++) {
      columnMetadata[i].setColumnIndex(i).setHeader(headers[i]);
    }
  }
  
  public ColumnInfo getColumnInfoByIndex(int i) {
    if (columnMetadata == null) {
      return null;
    }
    if (i < 0 || i >= columnMetadata.length) {
      return null;
    }

    return columnMetadata[i];
  }

  public ColumnInfo getColumnInfoByName(String name) {
    Integer i = getColumnIndexByName(name);
    if (i == null) {
      return null;
    }

    return getColumnInfoByIndex(i);
  }

  public Integer getColumnIndexByName(String name) {
    if (columnMetadata == null) {
      return null;
    }
    for (int i = 0; i < columnMetadata.length; i++) {
      if (columnMetadata[i].getHeader().equals(name)) {
        return i;
      }
    }
    return null;
  }
}
