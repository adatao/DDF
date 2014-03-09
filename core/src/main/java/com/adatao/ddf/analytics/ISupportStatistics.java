package com.adatao.ddf.analytics;

import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.IHandleDDFFunctionalGroup;

public interface ISupportStatistics extends IHandleDDFFunctionalGroup {

  public Summary[] getSummary() throws DDFException;

}
