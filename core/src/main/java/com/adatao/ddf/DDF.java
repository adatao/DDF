/**
 * Copyright 2014 Adatao, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *    
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package com.adatao.ddf;

import com.adatao.ddf.content.ColumnInfo;
import com.adatao.ddf.util.ISupportPhantomReference;
import com.adatao.ddf.util.PhantomReference;

/**
 * <p>
 * A Distributed DDF (DDF) has a number of key properties (metadata, representations, etc.) and
 * capabilities (self-compute basic statistics, aggregations, etc.).
 * </p>
 * <p>
 * </p>
 * 
 * @author ctn
 * 
 */
public class DDF implements ISupportPhantomReference {

  public DDF(ADDFHelper helper) {
    this.setHelper(helper);
    if (helper != null) helper.setDDF(this);

    PhantomReference.register(this);
  }


  private ADDFHelper mHelper;

  /**
   * Gets the underlying implementor of this DDF
   * 
   * @return
   */
  public ADDFHelper getHelper() {
    if (mHelper != null) return mHelper;
    else throw new UnsupportedOperationException("No implementor has been set");
  }

  /**
   * Sets the underlying implementor for this DDF
   * 
   * @param aDDFHelper
   */
  public void setHelper(ADDFHelper aDDFHelper) {
    this.mHelper = aDDFHelper;
  }

  public long nrows(){
    return this.getHelper().getMetaDataHandler().getNumRows();
  }
  
  public long ncols(){
    return this.getHelper().getMetaDataHandler().getNumColumns();
  }
    
  public ColumnInfo[] getColumnMetadata(){
    return this.getHelper().getMetaDataHandler().getColumnMetadata();
  }
  public DDF getRandomSample(int numSamples) {
    return this.getHelper().getMiscellanyHandler().getRandomSample(this, numSamples);
  }

  @Override
  // ISupportPhantomReference
  public void cleanup() {
    this.setHelper(null);
  }

  // /////////////////////////////////////
  // Content: Views & Representations
  // /////////////////////////////////////
  
  /**
   * Override to implement, e.g., in-memory caching support
   */
  public void cache() {
    // Nothing
  }
  
  /**
   * Override to implement, e.g., in-memory caching support
   */
  public void uncache() {
    // Nothing
  }


  // /////////////////////////////////////
  // ETL
  // /////////////////////////////////////


  // /////////////////////////////////////
  // Analytics
  // /////////////////////////////////////
}
