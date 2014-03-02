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
import com.adatao.ddf.DDF.ConfigConstant;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.analytics.MLSupporter.Model;
import com.adatao.ddf.analytics.ISupportML.IModel;
import com.adatao.ddf.content.APersistenceHandler.PersistenceUri;
import com.adatao.ddf.exception.DDFException;
import com.adatao.local.ddf.LocalObjectDDF;
import com.adatao.local.ddf.content.PersistenceHandler.PersistenceUri2;
import com.google.common.base.Strings;


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
      List<String> ddfs = ddf.getPersistenceHandler().listItems(namespace);
      Assert.assertNotNull(ddfs);
    }
  }

  @Test
  public void testPersistDDF() throws Exception {
    DDF ddf = new LocalObjectDDF<String>("Hello");

    PersistenceUri uri = ddf.persist();
    Assert.assertEquals("PersistenceUri must have '" + ConfigConstant.ENGINE_NAME_LOCAL + "' for engine/protocol",
        ConfigConstant.ENGINE_NAME_LOCAL.toString(), uri.getEngine());
    Assert.assertTrue("Persisted file must exist: " + uri, new File(uri.getPath()).exists());

    ddf.unpersist();
  }

  @Test
  public void testLoadDDF() throws Exception {
    DDF ddf1 = new LocalObjectDDF<String>("Hello");

    PersistenceUri uri = ddf1.persist();

    DDF ddf2 = DDFManager.doLoad(uri);
    Assert.assertNotNull(String.format("DDF from doLoad(%s) cannot be null", uri), ddf2);
    Assert.assertEquals("Created and loaded DDF names must be equal", ddf2.getName(), ddf1.getName());

    DDF ddf3 = DDFManager.get("local").load(uri);
    Assert.assertNotNull(String.format("DDF from doLoad(%s) cannot be null", uri), ddf3);
    Assert.assertEquals("Created and loaded DDF names must be equal", ddf3.getName(), ddf1.getName());

    PersistenceUri2 uri2 = new PersistenceUri2(uri);
    DDF ddf4 = DDFManager.get("local").load(uri2.getNamespace(), uri2.getName());
    Assert.assertNotNull(String.format("DDF from doLoad(%s) cannot be null", uri), ddf4);
    Assert.assertEquals("Created and loaded DDF names must be equal", ddf4.getName(), ddf1.getName());

    ddf1.unpersist();
  }

  @Test
  public void testPersistModel() throws DDFException {
    IModel model = new Model();

    PersistenceUri uri = model.persist();
    Assert.assertNotNull("Model persistence URI cannot be null", uri);
    Assert.assertFalse("Model persistence URI cannot be null or empty", Strings.isNullOrEmpty(uri.toString()));

    DDF ddf = DDFManager.doLoad(uri);
    Assert.assertNotNull(String.format("DDF from doLoad(%s) cannot be null", uri), ddf);

    @SuppressWarnings("unchecked")
    Model model2 = ((LocalObjectDDF<Model>) ddf).getObject();
    Assert.assertEquals("Models must be the same before and after persistence", model, model2);

    model.unpersist();
  }
}
