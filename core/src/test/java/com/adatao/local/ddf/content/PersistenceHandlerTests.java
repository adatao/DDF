/**
 * 
 */
package com.adatao.local.ddf.content;


import java.io.File;
import java.io.IOException;
import java.util.List;
import junit.framework.Assert;
import org.junit.Test;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.exception.DDFException;
import com.adatao.local.ddf.LocalObjectDDF;


/**
 *
 */
public class PersistenceHandlerTests {

  @Test
  public void testPersistenceDir() throws IOException, DDFException {
    DDFManager manager = DDFManager.get("local");
    DDF ddf = manager.newDDF();

    List<String> namespaces = ddf.getPersistenceHandler().listNamespaces();
    Assert.assertNotNull(namespaces);

    for (String namespace : namespaces) {
      List<String> ddfs = ddf.getPersistenceHandler().listDDFs(namespace);
      Assert.assertNotNull(ddfs);
    }
  }

  @Test
  public void testSaveDDF() throws Exception {
    DDF ddf = new LocalObjectDDF<String>("Hello");

    String uri = ddf.persist();
    Assert.assertTrue("Persisted file must exist: " + uri, new File(uri).exists());

    ddf.unpersist();
  }
}
