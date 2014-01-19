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
package com.adatao.DDF;

/**
 * A DDF has a number of key properties (metadata, representations, etc.) and capabilities
 * (self-compute basic statistics, aggregations, etc.).
 * 
 * @author ctn
 * 
 */
public class DataFrame {
  
  public DataFrame() {

  }

  private ADataFrameImplementor mImplementor;

  /**
   * Gets the underlying implementor of this DataFrame
   * 
   * @return
   */
  public ADataFrameImplementor getImplementor() {
    if (mImplementor != null) return mImplementor;
    else throw new UnsupportedOperationException("No implementor has been set");
  }

  /**
   * Sets the underlying implementor for this DataFrame
   * 
   * @param aDataFrameImplementor
   */
  public void setImplementor(ADataFrameImplementor aDataFrameImplementor) {
    this.mImplementor = aDataFrameImplementor;
  }

  public DataFrame getRandomSample(int numSamples) {
    return this.getImplementor().getMiscellanyHandler().getRandomSample(this, numSamples);
  }
}
