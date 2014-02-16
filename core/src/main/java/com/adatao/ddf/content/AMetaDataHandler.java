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

<<<<<<< HEAD
  public AMetaDataHandler(ADDFManager theDDFManager) {
    super(theDDFManager);
=======
  public AMetaDataHandler(ADDFManager ddfManager) {
    super(ddfManager);
>>>>>>> origin/master
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
<<<<<<< HEAD

=======
  private long mNumColumns = 0;
  private boolean bNumColumnsIsValid = false;
  private Schema mSchema;
  
>>>>>>> origin/master
  /**
   * Each implementation needs to come up with its own way to compute the row
   * count.
   * 
   * @return row count of a DDF
   */
  protected abstract long getNumRowsImpl();

  /**
   * Called to assert that the row count needs to be recomputed at next access
   */
  protected void invalidateNumRows() {
    bNumRowsIsValid = false;
  }

  @Override
  public long getNumRows() {
    if (!bNumRowsIsValid) {
      mNumRows = this.getNumRowsImpl();
      bNumRowsIsValid = true;
    }
    return mNumRows;
  }

<<<<<<< HEAD
=======
  @Override
  public long getNumColumns() {
    if (!bNumColumnsIsValid) {
      mNumColumns = this.getNumColumnsImpl();
      bNumColumnsIsValid = true;
    }
    return mNumColumns;
  }

  public void setSchema(Schema schema) {
    mSchema= schema;
  }

  public Schema getSchema() {
    return this.mSchema;
  }
>>>>>>> origin/master
}
