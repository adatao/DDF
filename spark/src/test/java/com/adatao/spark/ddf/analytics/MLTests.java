package com.adatao.spark.ddf.analytics;


import junit.framework.Assert;
import org.apache.spark.rdd.RDD;
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
    if (Strings.isNullOrEmpty(Config.getValue(ConfigConstant.ENGINE_NAME_SPARK.toString(), "kmeans"))) {
      Config.set(ConfigConstant.ENGINE_NAME_SPARK.toString(), "kmeans2",
          String.format("%s#%s", this.getClass().getName(), "dummyKMeans"));
    }
  }

  public static IModel dummyKMeans(RDD<Object> arg1, int arg2, Double arg3) {

    return null;
  }

  @Test
  public void testTrain() throws DDFException {
    this.initializeConfiguration();


    DDF ddf = DDFManager.get("spark").newDDF();
    Assert.assertNotNull("DDF cannot be null", ddf);

    // @huan, see {@link MLSupporter#convertDDF}

    // This uses the fully qualified class#method mechanism
    IModel model = ddf.ML.train("com.adatao.spark.ddf.analytics.MLTests#dummyKMeans", 1, 2.2);
    Assert.assertNotNull("Model cannot be null", model);

    // This uses the mapping config to go from "kmeans" to "com.adatao.spark.ddf.analytics.MLTests#dummyKMeans"
    model = ddf.ML.train("kmeans2", 1, 2.2);
    Assert.assertNotNull("Model cannot be null", model);
  }


}
