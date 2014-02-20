/**
 * 
 */
package com.adatao.ddf.content;

import java.util.UUID;

import com.adatao.ddf.ADDFFunctionalGroupHandler;
import com.adatao.ddf.ADDFManager;

/**
 * @author ctn
 * 
 */
public abstract class AMetaDataHandler extends ADDFFunctionalGroupHandler
    implements IHandleMetaData {


  public AMetaDataHandler(ADDFManager theDDFManager) {
    super(theDDFManager);
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
   * Called to assert that the column count needs to be recomputed at next access
   */
  protected void invalidateNumColumns() {
    bNumColumnsIsValid = false;
  }

  public ColumnInfo[] getColumnMetadata() {
    return columnMetadata;
  }


  @Override
  public long getNumRows() {
    if (!bNumRowsIsValid) {
      mNumRows = this.getNumRowsImpl();
      bNumRowsIsValid = true;
    }
    return mNumRows;
  }
}
