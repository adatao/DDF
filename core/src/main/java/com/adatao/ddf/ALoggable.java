/**
 * 
 */
package com.adatao.ddf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ctn
 * 
 */
public abstract class ALoggable {
  protected final Logger mLog = LoggerFactory.getLogger(this.getClass());
}
