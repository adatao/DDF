package com.adatao.ddf.util;


import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.misc.Config;
import com.adatao.ddf.util.ConfigHandler.Configuration;
import com.adatao.ddf.util.ConfigHandler.Configuration.Section;

public class ConfigHandlerTests {

  @Test
  public void testLoadConfig() throws Exception {
    DDFManager manager = DDFManager.get("basic"); // this will trigger a configuration loading
    Assert.assertEquals("basic", manager.getEngine());
    Assert.assertNotNull(Config.getConfigHandler());
    Assert.assertNotNull(Config.getConfigHandler().loadConfig());
  }

  @Test
  public void testReadConfig() throws Exception {
    DDFManager manager = DDFManager.get("basic"); // this will trigger a configuration loading
    Assert.assertEquals("basic", manager.getEngine());

    Configuration config = Config.getConfigHandler().getConfig();
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
