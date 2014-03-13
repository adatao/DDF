package com.adatao.ddf.analytics;


import junit.framework.Assert;
import org.junit.Test;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.analytics.ISupportML.IModel;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.Config;
import com.adatao.ddf.misc.Config.ConfigConstant;
import com.google.common.base.Strings;

public class MLTests {

  private void initializeConfiguration() {
    if (Strings.isNullOrEmpty(Config.getValue(ConfigConstant.ENGINE_NAME_BASIC.toString(), "kmeans"))) {
      Config.set(ConfigConstant.ENGINE_NAME_BASIC.toString(), "kmeans",
          String.format("%s#%s", this.getClass().getName(), "dummyKMeans"));
    }
  }

  public static IModel dummyKMeans(DDF arg1, int arg2, Double arg3) {
    return null;
   }

  @Test
  public void testTrain() throws DDFException {
    this.initializeConfiguration();

    DDF ddf = DDFManager.get("basic").newDDF();
    Assert.assertNotNull("DDF cannot be null", ddf);

    IModel model = ddf.ML.train("kmeans", 1, 2.2);
    Assert.assertNotNull("Model cannot be null", model);
  }


}
