/**
 * 
 */
package com.adatao.ddf.spark;

import org.junit.Assert;
import org.junit.Test;

import com.adatao.ddf.DDF;

/**
 * @author ctn
 * 
 */
public class IntegrationTests {

  /**
   * <p>
   * For our Spark implementation, HDFS is the default data source. Therefore, loading a DDF from
   * HDFS is simply passing in a SQL command.
   * </p>
   * <p>
   * The general load command has the following signature:
   * </p>
   * <code>
   *     DDF ddf = DDF.load(sqlCommand, source, schema);
   * </code>
   */
  @Test
  public void testLoadFromHdfs() {
    DDF ddf = DDF.load(sqlCommand);

    // Do something to verify that we have successfully loaded the data
    Assert.assertNotNull("Loaded DDF should not be null", ddf);

    Assert.assertNotSame("Loaded DDF should have some rows", 0, ddf.getNumRows());
    Assert.assertNotSame("Loaded DDF should have some columns", 0, ddf.getNumColumns());
    Assert.assertNotNull("Loaded DDF should have a schema", ddf.getSchema());
  }

}
