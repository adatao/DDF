package com.adatao.ddf.content;

import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;

public interface IHandleMutability extends IHandleDDFFunctionalGroup {

  public void setMutable(boolean isMutable);
  
  public boolean isMutable();
  
  public DDF updateInplace(DDF ddf) throws DDFException;
}
