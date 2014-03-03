package com.adatao.ddf.analytics;


import junit.framework.Assert;
import org.junit.Test;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.analytics.ISupportML.IModel;
import com.adatao.ddf.exception.DDFException;

public class MLTests {

  @Test
  public void testTrain() throws DDFException {
    DDF ddf = DDFManager.get("local").newDDF();
    Assert.assertNotNull("DDF cannot be null", ddf);

    IModel model = ddf.ML.train("kmeans");
    Assert.assertNotNull("Model cannot be null", model);
  }
}
