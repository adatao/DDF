package com.adatao.spark.ddf.etl;

import java.util.List;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.analytics.Summary;
import com.adatao.ddf.exception.DDFException;

public class TransformationHandlerTest {
  
  @Test
  @Ignore
  public void testTransformNativeRserve() throws DDFException {
    DDFManager manager = DDFManager.get("spark");
    DDF ddf = manager
        .sql2ddf("select year, month, dayofweek, deptime, arrtime, distance, arrdelay, depdelay from airline");
    DDF newddf = ddf.Transform.transformNativeRserve("newcol = deptime / arrtime");
    System.out.println("name "+ ddf.getName());
    System.out.println("newname "+ newddf.getName());
    List<String> res =  ddf.Views.firstNRows(10);
    Assert.assertNotNull(newddf);
    Assert.assertEquals("newcol", newddf.getColumnName(9));
    Assert.assertEquals(10, res.size());
    manager.shutdown();
  }
  
  @Test
  public void testTransformScaleMinMax() throws DDFException {
    DDFManager manager = DDFManager.get("spark");
    DDF ddf = manager
        .sql2ddf("select year, month, dayofweek, deptime, arrtime, distance, arrdelay, depdelay from airline");
    DDF newddf = ddf.Transform.transformScaleMinMax();
    Summary[] summaryArr = newddf.getSummary();
    Assert.assertTrue(summaryArr[0].min() < 1);
    Assert.assertTrue(summaryArr[0].max() == 1);
    
    manager.shutdown();
    
  }

}
