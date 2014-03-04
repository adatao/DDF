package com.adatao.local.ddf;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import junit.framework.Assert;
import org.junit.Test;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.Vector;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.exception.DDFException;

public class LocalDDFTests {

  DDFManager mManager;


  private DDFManager getDDFManager() throws DDFException {
    if (mManager == null) mManager = DDFManager.get("local");
    return mManager;
  }

  private DDF getTestDDF() throws DDFException {
    List<Object[]> list = new ArrayList<Object[]>();
    list.add(new Object[] { "Last", "Nguyen" });
    list.add(new Object[] { "First", "Christopher" });
    String namespace = null; // use default
    String name = this.getClass().getSimpleName();
    Schema schema = new Schema(name, "name string, value string");
    DDF ddf = ((LocalDDFManager) this.getDDFManager()).newDDF(list, namespace, name, schema);
    return ddf;
  }

  @Test
  public void testCreateDDF() throws DDFException {
    Assert.assertNotNull("DDFManager cannot be null", this.getDDFManager());

    DDF ddf = this.getDDFManager().newDDF();
    Assert.assertNotNull("DDF cannot be null", ddf);

    Assert.assertNotNull(ddf.getNamespace());
    Assert.assertNotNull(ddf.getName());

    DDF ddf2 = this.getTestDDF();
    Assert.assertNotNull("DDF cannot be null", ddf2);
  }

  @Test
  public void testDDFRepresentations() throws DDFException {
    DDF ddf = this.getTestDDF();
    Assert.assertNotNull("DDF cannot be null", ddf);

    Object repr = ddf.getRepresentationHandler().getDefault();
    Assert.assertNotNull("Representation cannot be null", repr);

    @SuppressWarnings("unchecked")
    List<Object[]> list = (List<Object[]>) repr;
    Assert.assertNotNull("List cannot be null", list);

    for (Object[] row : list) {
      Assert.assertNotNull("Row in list cannot be null", row);
    }
  }

  @Test
  public void testIterators() throws DDFException {
    DDF ddf = this.getTestDDF();
    Assert.assertNotNull("DDF cannot be null", ddf);

    @SuppressWarnings("unchecked")
    Iterator<Object[]> rowIter = (Iterator<Object[]>) ddf.getRowIterator();
    Assert.assertNotNull("Row iterator cannot be null", rowIter);
    while (rowIter.hasNext()) {
      Object[] row = rowIter.next();
      Assert.assertNotNull("Row  cannot be null", row);
      for (Object element : row) {
        Assert.assertNotNull("Elment  cannot be null", element);
      }
    }

    @SuppressWarnings("unchecked")
    Iterator<Object> eleIter = (Iterator<Object>) ddf.getElementIterator("name");
    Assert.assertNotNull("Element iterator cannot be null", eleIter);
    while (eleIter.hasNext()) {
      Object element = eleIter.next();
      Assert.assertNotNull("Element cannot be null", element);
      System.out.println(element);
    }

    Vector<String> vector = new Vector<String>(ddf, "value");
    Assert.assertNotNull("Vector cannot be null", vector);
    Iterator<String> itemIter = (Iterator<String>) vector.iterator();
    Assert.assertNotNull("Item iterator cannot be null", itemIter);
    while (itemIter.hasNext()) {
      Object item = itemIter.next();
      Assert.assertNotNull("Item cannot be null", item);
      System.out.println(item);
    }

  }
}
