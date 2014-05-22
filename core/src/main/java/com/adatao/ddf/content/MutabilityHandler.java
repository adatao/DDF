package com.adatao.ddf.content;

import com.adatao.ddf.DDF;
import com.adatao.ddf.misc.ADDFFunctionalGroupHandler;

public class MutabilityHandler extends ADDFFunctionalGroupHandler implements IHandleMutability {

  public MutabilityHandler(DDF theDDF) {
    super(theDDF);
    // TODO Auto-generated constructor stub
  }

  @Override
  public DDF getDDF() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setDDF(DDF theDDF) {
    // TODO Auto-generated method stub
    
  }

  private boolean isMutable = false;

  @Override
  public boolean isMutable() {
    return isMutable;
  }

  @Override
  public void setMutable(boolean isMutable) {
    this.isMutable = isMutable;
  }
  
  
}
