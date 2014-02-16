package com.adatao.ddf.content;

import java.util.UUID;

public interface IHandleMetaData {

  public UUID getId();

  public void setId(UUID id);

  public long getNumRows();
<<<<<<< HEAD
=======

  public long getNumColumns();

  public Schema getSchema();

  public void setSchema(Schema shema);
>>>>>>> origin/master
}
