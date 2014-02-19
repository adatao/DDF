/**
 * 
 */
package com.adatao.ddf;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalINIConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;


/**
 * @author ctn
 * 
 */
public class DDFConfig {

  /**
   * Stores DDF configuration information from ddf.cfg
   */
  static class Config {

    static class Section {
      private Map<String, String> mEntries = new HashMap<String, String>();

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

    public Section get(String sectionName) {
      if (mSections == null) return null;

      Section section = mSections.get(safeToLower(sectionName));

      if (section == null) {
        section = new Section();
        mSections.put(safeToLower(sectionName), section);
      }

      return section;
    }

    public void remove(String sectionName) {
      if (mSections == null) return;

      this.get(sectionName).clear();
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
   * @return the default {@link ADDFManager} to be used when the user calls static methods of DDF
   * @throws ConfigurationException
   */
  public static Config loadConfig() throws InstantiationException, IllegalAccessException, ClassNotFoundException,
      ConfigurationException {

    Config resultConfig = new Config();

    String configFileName = System.getenv("DDF_INI");
    if (configFileName == null) configFileName = DDF.DEFAULT_CONFIG_FILE_NAME;

    HierarchicalINIConfiguration config = new HierarchicalINIConfiguration(configFileName);

    @SuppressWarnings("unchecked")
    Set<String> sectionNames = config.getSections();
    for (String sectionName : sectionNames) {
      SubnodeConfiguration section = config.getSection(sectionName);
      if (section != null) {
        Config.Section resultSection = resultConfig.get(sectionName);

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

    return resultConfig;
  }

}
