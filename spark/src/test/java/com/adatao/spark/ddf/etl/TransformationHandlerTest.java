package com.adatao.spark.ddf.etl;

import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.analytics.Summary;
import com.adatao.ddf.exception.DDFException;

public class TransformationHandlerTest {
  private DDFManager manager;
  private DDF ddf;


  @Before
  public void setUp() throws Exception {
    manager = DDFManager.get("spark");
    ddf = manager
        .sql2ddf("select year, month, dayofweek, deptime, arrtime, distance, arrdelay, depdelay from airline");
  } 
  
  @Test
  @Ignore
  public void testTransformNativeRserve() throws DDFException {

    DDF newddf = ddf.Transform.transformNativeRserve("newcol = deptime / arrtime");
    System.out.println("name "+ ddf.getName());
    System.out.println("newname "+ newddf.getName());
    List<String> res =  ddf.Views.firstNRows(10);
    Assert.assertNotNull(newddf);
    Assert.assertEquals("newcol", newddf.getColumnName(9));
    Assert.assertEquals(10, res.size());
  }
  
  @Test
  @Ignore
  public void testTransformScaleMinMax() throws DDFException {

    DDF newddf = ddf.Transform.transformScaleMinMax();
    Summary[] summaryArr = newddf.getSummary();
    Assert.assertTrue(summaryArr[0].min() < 1);
    Assert.assertTrue(summaryArr[0].max() == 1);
    
  }
  
  @Test
  public void testTransformScaleStandard() throws DDFException {

    DDF newddf = ddf.Transform.transformScaleStandard();
    Assert.assertNotNull(newddf);
    
  }
  
  @After
  public void closeTest() {
    manager.shutdown();
  }

}
