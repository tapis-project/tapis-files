package edu.utexas.tacc.tapis.files.lib.config;

import java.util.HashMap;
import java.util.Map;


/**
 *
 */
public class Settings {

  private Map<String, String> props = (Map) System.getProperties();
  private Map<String, String> envs = System.getenv();
  private Map<String, String> settings;


  /**
   * @param key
   * @return value
   */
  public String get(String key) {
    if (settings == null) {
      settings = new HashMap();
      settings.putAll(envs);
      settings.putAll(props);
    }
    return settings.get(key);
  }


}
