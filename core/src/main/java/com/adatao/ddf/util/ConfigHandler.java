/**
 * 
 */
package com.adatao.ddf.util;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalINIConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;

import com.adatao.ddf.ADDFFunctionalGroupHandler;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.DDF;
import com.adatao.ddf.util.ConfigHandler.Config.Section;


/**
 * @author ctn
 * 
 */
public class ConfigHandler extends ADDFFunctionalGroupHandler implements IHandleConfig {

  public ConfigHandler() {
    super(null);
  }

  public ConfigHandler(DDF theDDF) {
    super(theDDF);
  }

  public static final String CONFIG_FILE_ENV_VAR = "DDF_INI";
  public static final String DEFAULT_CONFIG_FILE_NAME = "ddf.ini";
  public static final String CONFIG_SEARCH_DIR_NAME = "conf";

  private Config mConfig;

  @Override
  public Config getConfig() {
    if (mConfig == null) try {
      mConfig = this.loadConfig();

    } catch (Exception e) {
      mLog.error("Unable to initialize configuration", e);
    }

    return mConfig;
  }

  /**
   * Stores DDF configuration information from ddf.ini
   */
  public static class Config {

    public static class Section {
      private Map<String, String> mEntries = new HashMap<String, String>();

      public Map<String, String> getEntries() {
        return mEntries;
      }

      public String get(String key) {
        return mEntries.get(safeToLower(key));
      }

      public void set(String key, String value) {
        mEntries.put(safeToLower(key), value);
      }

      public void remove(String key) {
        mEntries.remove(safeToLower(key));
      }

      public void clear() {
        mEntries.clear();
      }
    }

    private Map<String, Section> mSections;

    public Config() {
      this.reset();
    }

    private static String safeToLower(String s) {
      return s == null ? null : s.toLowerCase();
    }

    public Map<String, Section> getSections() {
      return mSections;
    }

    public Map<String, String> getSettings(String sectionName) {
      Section section = this.getSection(sectionName);
      return section.getEntries();
    }

    public Section getSection(String sectionName) {
      if (mSections == null) return null;

      Section section = mSections.get(safeToLower(sectionName));

      if (section == null) {
        section = new Section();
        mSections.put(safeToLower(sectionName), section);
      }

      return section;
    }

    public void removeSection(String sectionName) {
      if (mSections == null) return;

      this.getSection(sectionName).clear();
      mSections.remove(safeToLower(sectionName));
    }

    public void reset() {
      if (mSections != null) {
        for (Section section : mSections.values()) {
          section.clear();
        }
      }
      mSections = new HashMap<String, Section>();
    }
  }

  /**
   * Load configuration from ddf.ini, or the file name specified by the environment variable
   * DDF_INI.
   * 
   * @throws ClassNotFoundException
   * @throws IllegalAccessException
   * @throws InstantiationException
   * 
   * @return the default {@link DDFManager} to be used when the user calls static methods of DDF
   * @throws ConfigurationException
   */
  @Override
  public Config loadConfig() throws Exception {

    Config resultConfig = new Config();

    String configFileName = System.getenv(CONFIG_FILE_ENV_VAR);
    if (configFileName == null) configFileName = this.locateConfigFileName();

    // TODO: load a default, built-in configuration, even if we can't find the config file

    HierarchicalINIConfiguration config = new HierarchicalINIConfiguration(configFileName);

    @SuppressWarnings("unchecked")
    Set<String> sectionNames = config.getSections();
    for (String sectionName : sectionNames) {
      SubnodeConfiguration section = config.getSection(sectionName);
      if (section != null) {
        Config.Section resultSection = resultConfig.getSection(sectionName);

        @SuppressWarnings("unchecked")
        Iterator<String> keys = section.getKeys();
        while (keys.hasNext()) {
          String key = keys.next();
          String value = section.getString(key);
          if (value != null) {
            resultSection.set(key, value);
          }
        }
      }
    }

    mConfig = resultConfig;
    return mConfig;
  }

  /**
   * Search in current dir and working up, looking for the config file
   * 
   * @return
   * @throws IOException
   */
  private String locateConfigFileName() throws IOException {
    String configFileName = DEFAULT_CONFIG_FILE_NAME;
    String curDir = new File(".").getCanonicalPath();

    String path = null;

    // Go for at most 10 levels up
    for (int i = 0; i < 10; i++) {
      path = String.format("%s/%s", curDir, configFileName);
      if (Utils.fileExists(path)) break;

      String dir = String.format("%s/%s", curDir, CONFIG_SEARCH_DIR_NAME);
      if (Utils.dirExists(dir)) {
        path = String.format("%s/%s", dir, configFileName);
        if (Utils.fileExists(path)) break;
      }

      curDir = String.format("%s/..", curDir);
    }

    // System.out.printf("Found config at %s\n", path);
    return path;
  }

  public Section getSection(String sectionName) {
    return this.getConfig().getSection(sectionName);
  }

  @Override
  public String getValue(String sectionName, String key) {
    Section section = this.getConfig().getSection(sectionName);
    return section == null ? null : section.get(key);
  }

  @Override
  public Map<String, Section> getSections() {
    return this.getConfig().getSections();
  }

  @Override
  public Map<String, String> getSettings(String sectionName) {
    return this.getConfig().getSettings(sectionName);
  }
}
