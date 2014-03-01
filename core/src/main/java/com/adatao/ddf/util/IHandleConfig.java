package com.adatao.ddf.util;


import java.util.Map;
import com.adatao.ddf.util.ConfigHandler.Config;
import com.adatao.ddf.util.ConfigHandler.Config.Section;

public interface IHandleConfig {

  /**
   * Returns a file name, URI, or some other description of where the configuration info comes from.
   * 
   * @return
   */
  public String getSource();

  /**
   * Loads/reloads the config file
   * 
   * @param configDirectory
   * @return the loaded {@link Config} object
   * @throws Exception
   */
  public Config loadConfig() throws Exception;

  public Config getConfig();

  public void setConfig(Config theConfig);

  public Map<String, Section> getSections();

  public Section getSection(String sectionName);

  public Map<String, String> getSettings(String sectionName);

  public String getValue(String sectionName, String key);
}
