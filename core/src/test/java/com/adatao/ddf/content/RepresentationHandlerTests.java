package com.adatao.ddf.content;


import java.util.ArrayList;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import junit.framework.Assert;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.exception.DDFException;


/**
 * Unit tests for generic DDF.
 */

public class RepresentationHandlerTests {

  private static List<String> list = new ArrayList<String>();


  @BeforeClass
  public static void setupFixture() {
    list.add("a");
    list.add("b");
    list.add("c");
  }

  @AfterClass
  public static void shutdownFixture() {}


  @Test
  public void testRepresentDDF() throws DDFException {
    DDFManager manager = DDFManager.get("basic");
    DDF ddf = manager.newDDF();

    IHandleRepresentations handler = ddf.getRepresentationHandler();

    handler.reset();
    Assert.assertNull("There should not be any existing representations", handler.get(list.get(0).getClass()));

    handler.set(list, List.class, String.class);
    Assert.assertNotNull("There should now be a representation of type <List,String>",
        handler.get(List.class, String.class));

    handler.add(list, List.class, String.class);
    Assert.assertNotNull("There should now be a representation of type <List,String>",
        handler.get(List.class, String.class));

    handler.remove(List.class, String.class);
    Assert.assertNull("There should now be no representation of type <List,String>",
        handler.get(List.class, String.class));
  }
}
