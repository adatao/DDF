/**
 * 
 */
package com.adatao.ddf.content;


import com.adatao.ddf.ADDFFunctionalGroupHandler;
import com.adatao.ddf.ADDFManager;

/**
 * @author ctn
 * 
 */
public abstract class AViewHandler extends ADDFFunctionalGroupHandler implements IHandleViews {

  public AViewHandler(ADDFManager theContainer) {
    super(theContainer);
  }
}
