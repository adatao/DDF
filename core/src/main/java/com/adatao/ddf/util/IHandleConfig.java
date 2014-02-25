package com.adatao.ddf.util;

import java.util.Map;

import com.adatao.ddf.IHandleDDFFunctionalGroup;
import com.adatao.ddf.util.ConfigHandler.Config;
import com.adatao.ddf.util.ConfigHandler.Config.Section;

public interface IHandleConfig extends IHandleDDFFunctionalGroup {

  public Config loadConfig() throws Exception;

  public Config getConfig();

  public Map<String, Section> getSections();

  public Section getSection(String sectionName);

  public Map<String, String> getSettings(String sectionName);

  public String getValue(String sectionName, String key);
}
