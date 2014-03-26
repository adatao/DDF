package com.adatao.spark.ddf.analytics;


import org.junit.Assert;
import org.junit.Test;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.content.Schema.ColumnClass;
import com.adatao.ddf.exception.DDFException;

public class BinningHandlerTest {

  @Test
  public void testBinning() throws DDFException {
    DDFManager manager = DDFManager.get("spark");
    DDF ddf = manager
        .sql2ddf("select year, month, dayofweek, deptime, arrtime,origin, distance, arrdelay, depdelay, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay from airline");
    DDF newddf = ddf.binning("month", "EQUALINTERVAL", 2, null, true, true);
    Assert.assertEquals(ColumnClass.FACTOR, newddf.getSchemaHandler().getColumn("month").getColumnClass());
    Assert.assertEquals(2, newddf.getSchemaHandler().getColumn("month").getOptionalFactor().getLevelMap().size());
    
    DDF ddf1 = ddf.binning("month", "custom", 0, new double[]{2, 4, 6, 8}, true, true);
    //{'[2,4]'=1, '(4,6]'=2, '(6,8]'=3}
    Assert.assertTrue(ddf1.getSchemaHandler().getColumn("month").getOptionalFactor().getLevelMap().get("'[2,4]'")==1);
    manager.shutdown();
  }

}
