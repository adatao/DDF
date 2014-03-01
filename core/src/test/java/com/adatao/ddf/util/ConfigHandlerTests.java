package com.adatao.ddf.util;


import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.util.ConfigHandler.Config;
import com.adatao.ddf.util.ConfigHandler.Config.Section;

public class ConfigHandlerTests {

  @Test
  public void testLoadConfig() throws Exception {
    DDFManager manager = DDFManager.get("local"); // this will trigger a configuration loading
    Assert.assertEquals("local", manager.getEngine());
    Assert.assertNotNull(DDF.getConfigHandler());
    Assert.assertNotNull(DDF.getConfigHandler().loadConfig());
  }

  @Test
  public void testReadConfig() throws Exception {
    DDFManager manager = DDFManager.get("local"); // this will trigger a configuration loading
    Assert.assertEquals("local", manager.getEngine());

    Config config = DDF.getConfigHandler().getConfig();
    Assert.assertNotNull(config);

    Map<String, Section> sections = config.getSections();
    Assert.assertNotNull(sections);

    for (String sectionName : sections.keySet()) {
      Map<String, String> settings = config.getSettings(sectionName);
      Assert.assertNotNull(settings);
    }
  }

  @Test
  public void testWriteConfig() {

  }
}
