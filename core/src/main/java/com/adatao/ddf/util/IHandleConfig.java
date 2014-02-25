package com.adatao.ddf.util;

import java.util.Map;

import com.adatao.ddf.IHandleDDFFunctionalGroup;
import com.adatao.ddf.util.ConfigHandler.Config;
import com.adatao.ddf.util.ConfigHandler.Config.Section;

public interface IHandleConfig extends IHandleDDFFunctionalGroup {

  /**
   * Returns a file name, URI, or some other description of where the configuration info comes from.
   * 
   * @return
   */
  public String getSource();

  /**
   * Loads/reloads the config file
   * 
   * @return the loaded {@link Config} object
   * @throws Exception
   */
  public Config loadConfig() throws Exception;

  public Config getConfig();

  public Map<String, Section> getSections();

  public Section getSection(String sectionName);

  public Map<String, String> getSettings(String sectionName);

  public String getValue(String sectionName, String key);
}
