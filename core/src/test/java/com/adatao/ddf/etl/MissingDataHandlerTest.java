package com.adatao.ddf.etl;


import org.junit.Assert;
import org.junit.Test;
import com.adatao.ddf.etl.IHandleMissingData.Axis;
import com.adatao.ddf.etl.IHandleMissingData.NAChecking;

public class MissingDataHandlerTest {

  @Test
  public void testEnums() {
    Assert.assertEquals(Axis.COLUMN, Axis.fromString("column"));
    Assert.assertEquals(NAChecking.ALL, NAChecking.fromString("all"));
    Assert.assertTrue(Axis.fromString("column") == Axis.COLUMN);
  }

}
