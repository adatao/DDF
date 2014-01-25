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

  protected abstract long getNumRowsImpl();

  protected abstract long getNumColumnsImpl();

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
}
