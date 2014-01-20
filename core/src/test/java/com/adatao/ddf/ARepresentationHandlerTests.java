package com.adatao.ddf;

import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Unit tests for DDF.
 */
public class ARepresentationHandlerTests {
  public static class Helper extends ADDFHelper {
    public Helper(DDF ddf) {
      super(ddf);
      this.setRepresentationHandler(new Handler(this));
    }
  }

  public static class Handler extends ARepresentationHandler {
    public Handler(ADDFHelper container) {
      super(container);
    }
  }

  private static DDF ddf = new DDF(new Helper(null));
  private static ADDFHelper helper = ddf.getHelper();
  private static IHandleRepresentations handler = helper.getRepresentationHandler();

  private static List<String> list = new ArrayList<String>();


  @BeforeClass
  public static void setupFixture() {
    Assert.assertNotNull("Newly instantiated DDF from RDD should not be null", ddf);

    list.add("a");
    list.add("b");
    list.add("c");
  }

  @AfterClass
  public static void shutdownFixture() {
  }


  @Test
  public void testRepresentDDF() {
    handler.reset();
    Assert.assertNull("There should not be any existing representations",
        handler.get(list.getClass(), list.get(0).getClass()));

    handler.set(list, list.getClass(), list.get(0).getClass());
    Assert.assertNotNull("There should now be a representation of type <List,String>",
        handler.get(list.getClass(), list.get(0).getClass()));

    handler.add(list, list.getClass(), list.get(0).getClass());
    Assert.assertNotNull("There should now be a representation of type <List,String>",
        handler.get(list.getClass(), list.get(0).getClass()));

    handler.remove(list.getClass(), list.get(0).getClass());
    Assert.assertNull("There should now be no representation of type <List,String>",
        handler.get(list.getClass(), list.get(0).getClass()));
  }
}
